# The entrypoint for VxIngest
# Responsible for checking the existing jobs documents and determining which need to be run

import argparse
import contextlib
import logging
import os
import shutil
import sys
import tarfile
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from multiprocessing import Queue, set_start_method
from pathlib import Path
from typing import TypedDict

import yaml
from couchbase.auth import PasswordAuthenticator  # type: ignore
from couchbase.cluster import Cluster  # type: ignore
from couchbase.exceptions import CouchbaseException, TimeoutException  # type: ignore
from couchbase.options import (  # type: ignore
    ClusterOptions,
    ClusterTimeoutOptions,
    QueryOptions,
)
from prometheus_client import CollectorRegistry, Counter, Gauge, write_to_textfile

from vxingest.ctc_to_cb.run_ingest_threads import VXIngest as CTCIngest
from vxingest.grib2_to_cb.run_ingest_threads import VXIngest as GRIBIngest
from vxingest.log_config import (
    add_logfile,
    configure_logging,
    remove_logfile,
    worker_log_configurer,
)
from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest as NetCDFIngest
from vxingest.partial_sums_to_cb.run_ingest_threads import VXIngest as PartialSumsIngest

# from vxingest.prepbufr_to_cb.run_ingest_threads import VXIngest as PrepbufrIngest

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)

# Configure prometheus metrics
# Note - we may need to import prometheus's multiprocessing libraries

# Create a registry we can write to a file
prom_registry = CollectorRegistry()

# Use a gauge because we're doing 1 file per job run, a histogram could be more appropriate.
# Note - if we used a historgram or summary, we could apply a decorator directly to the function we're interested in
prom_duration = Gauge(
    "run_ingest_duration",
    "The duration of an ingest run, in seconds",
    registry=prom_registry,
)
prom_successes = Counter(
    "run_ingest_success_count",
    "The number of successful ingest jobs",
    registry=prom_registry,
)
prom_failures = Counter(
    "run_ingest_failure_count",
    "The number of failed ingest jobs",
    registry=prom_registry,
)
prom_last_success = Gauge(
    "job_last_success_unixtime",
    "Last time a batch job successfully finished",
    registry=prom_registry,
)


def process_cli():
    """Processes the following flags

    -c - credentials file path (optional)
    -m - metrics directory path
    -o - output directory path
    -x - "transfer" directory path
    -l - log directory path
    -j - jobid (optional)
    -s - start epoch (optional)
    -e - end epoch (optional)
    -f - file_pattern (optional)
    -t - threads (optional)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job_id",
        type=str,
        required=False,
        help="An optional couchbase job document id",
    )
    parser.add_argument(
        "-c",
        "--credentials_file",
        type=Path,
        required=False,
        default="config.yaml",
        help="Path to the credentials file",
    )
    parser.add_argument(
        "-m",
        "--metrics_dir",
        type=Path,
        required=True,
        help="Path to write metrics files",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=Path,
        required=True,
        help="Path to write output couchbase JSON files",
    )
    parser.add_argument(
        "-x",
        "--transfer_dir",
        type=Path,
        required=True,
        help="Path to the 'transfer directory'",
    )
    parser.add_argument(
        "-l",
        "--log_dir",
        type=Path,
        required=True,
        help="Path to write log files",
    )
    parser.add_argument(
        "-s",
        "--start_epoch",
        type=int,
        required=False,
        default=0,
        help="The first epoch to process jobs for, inclusive. Only valid for CTC & SUM jobs",
    )
    parser.add_argument(
        "-e",
        "--end_epoch",
        type=int,
        required=False,
        default=sys.maxsize,
        help="The last epoch to process jobs for, exclusive. Only valid for CTC & SUM jobs",
    )
    parser.add_argument(
        "-f",
        "--file_pattern",
        type=str,
        required=False,
        default="*",
        help="The filename pattern to use when searching for files to process. Only valid for GRIB & NetCDF jobs - this arg can be overridden by the template.",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        required=False,
        default=determine_num_processes(),
        help=f"The number of threads to use. Default is {determine_num_processes()}.",
    )
    # get the command line arguments
    args = parser.parse_args()
    return args


def get_credentials(path: Path) -> dict[str, str]:
    """
    Loads a YAML config file from the given path.

    Returns a dictionary of values from the file
    """

    # Check the file exists
    if not path.is_file():
        raise FileNotFoundError(f"Credentials file can not be found: {path}")

    # Load the file
    config: dict[str, str] = {}
    with path.open() as file:
        config = yaml.load(file, yaml.SafeLoader)

    # Check that nothing's missing
    required_keys = [
        "cb_host",
        "cb_user",
        "cb_password",
        "cb_bucket",
        "cb_scope",
        "cb_collection",
    ]
    for key in required_keys:
        if key not in config:
            logger.error(f"Missing required field {key} in config file {path}")
            raise KeyError

    return config


def create_dirs(paths: list[Path]) -> None:
    """Creates directory structure"""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


class JobRunCriterion(TypedDict):
    """A type class describing the criterion for running a job, and the
    expected return result from Couchbase"""

    id: str
    name: str
    offset_minutes: int
    run_priority: int
    subType: str


def get_runtime_job_criteria(
    cluster: Cluster,
    creds: dict[str, str],
    job_id: str | None = None,
) -> JobRunCriterion | None:
    """
    Queries the Couchbase database for a specific job document by its ID.
    This is used to retrieve the newer runtime job documents type "JS" for processing.

    Args:
        cluster (Cluster): The Couchbase cluster instance to use for querying.
        creds (dict[str, str]): A dictionary containing Couchbase credentials, including 'cb_bucket' and 'cb_scope'.
        job_id str | None = None, optional): The ID of the job document to retrieve. Must be provided.

    Returns:
        processDoc | None: The job document if found, otherwise None.

    Raises:
        ValueError: If job_id is not provided.
    """
    if not job_id:
        raise ValueError("job_id must be provided to get_runtime_job_doc")

    # Build the query to fetch a specific job document by ID
    query = f"""
        SELECT meta().id AS id,
        LOWER(META().id) as name,
        subType,
        subset,
        processSpecIds,
        status
        FROM {creds["cb_bucket"]}._default.RUNTIME
        WHERE id='{job_id}'
            AND type = 'JS'
            AND version = 'V01'
            AND (status = 'active'  OR status = 'Test') """

    row_iter = cluster.query(query, QueryOptions(read_only=True))  # type: ignore[assignment]
    for row in row_iter:
        return row
    return None


def get_older_job_doc_criteria(
    cluster: Cluster,
    creds: dict[str, str],
    job_id: str | None = None,
) -> list[JobRunCriterion]:
    """
    Queries Couchbase for job documents based on the provided parameters. This is used to
    retrieve the older type "JOB" or "JOB-TEST" documents.

    Depending on the arguments and environment variables, this function performs one of the following:
    - If a `job_id` is provided, fetches the specific job document with that ID.
    - If the environment variable `VXINGEST_IGNORE_JOB_SCHEDULE` is set to "true", fetches all active job documents regardless of schedule.
    - Otherwise, fetches all active job documents whose schedule is valid within the past 15 minutes.

    Args:
        cluster (Cluster): The Couchbase cluster instance to use for querying.
        creds (dict[str, str]): A dictionary containing Couchbase credentials and configuration, including bucket, scope, and collection names.
        job_id (str | None, optional): The ID of a specific job document to fetch. If not provided, fetches jobs based on schedule and status.

    Returns:
        list[processDoc]: A list of job documents matching the query criterion.
    """
    """Queries Couchbase for the given job doc or job docs in need of processing if no job ID is given"""

    def build_query_for_job_id(job_id: str) -> str:
        """Builds a query to fetch a specific job document by ID"""
        # fmt: off
        # Disable formatting to keep the Couchbase query readable
        # the older job documents (type="JOB" or type="JOB-TEST") are kept in the COMMON collection along with other metadata.
        # Eventually they will be replaced by newer JS documents that are in the RUNTIM collection
        # and deleted entirely.
        return (
            "SELECT meta().id AS id, "
                "LOWER(META().id) as name, "
                "run_priority, "
                "offset_minutes, "
                "LOWER(subType) as subType "
            f"FROM {creds['cb_bucket']}._default.COMMON "
            f"WHERE id='{job_id}' "
                "AND (type = 'JOB-TEST' or type = 'JOB') "
                "AND version = 'V01' "
                "AND CONTAINS(status, 'active') "
        )
        # fmt: on

    def build_query_for_scheduled_active_jobs() -> str:
        """Builds a query to fetch all active job documents with 'schedule' field valid within the past 15 minutes"""
        # fmt: off
        # Disable formatting to keep the Couchbase query readable
        return (
            "SELECT meta().id AS id, "
                "LOWER(META().id) as name, "
                "run_priority, "
                "offset_minutes, "
                "LOWER(subType) as subType "
            f"FROM {creds['cb_bucket']}._default.COMMON "
            "LET millis = ROUND(CLOCK_MILLIS()), "
                "sched = SPLIT(schedule,' '), "
                "minute = CASE WHEN sched[0] = '*' THEN DATE_PART_MILLIS(millis, 'minute', 'UTC') ELSE TO_NUMBER(sched[0]) END, "
                "hour = CASE WHEN sched[1] = '*' THEN DATE_PART_MILLIS(millis, 'hour', 'UTC') ELSE TO_NUMBER(sched[1]) END, "
                "day = CASE WHEN sched[2] = '*' THEN DATE_PART_MILLIS(millis, 'day', 'UTC') ELSE TO_NUMBER(sched[2]) END, "
                "month = CASE WHEN sched[3] = '*' THEN DATE_PART_MILLIS(millis, 'month', 'UTC') ELSE TO_NUMBER(sched[3]) END, "
                "year = CASE WHEN sched[4] = '*' THEN DATE_PART_MILLIS(millis, 'year', 'UTC') ELSE TO_NUMBER(sched[4]) END "
            "WHERE type='JOB' "
                "AND version='V01' "
                "AND status='active' "
                "AND DATE_PART_MILLIS(millis, 'year', 'UTC') = year "
                "AND DATE_PART_MILLIS(millis, 'month', 'UTC') = month "
                "AND DATE_PART_MILLIS(millis, 'hour', 'UTC') = hour "
                "AND DATE_PART_MILLIS(millis, 'day', 'UTC') = day "
                "AND IDIV(DATE_PART_MILLIS(millis, 'minute', 'UTC'), 15) = IDIV(minute, 15) "
            "ORDER BY offset_minutes, "
                    "run_priority "
        )
        # fmt: on

    def build_query_for_active_jobs() -> str:
        """Builds a query to fetch all active job documents"""
        # fmt: off
        # Disable formatting to keep the Couchbase query readable
        return (
            "SELECT meta().id AS id, "
                "LOWER(META().id) as name, "
                "run_priority, "
                "offset_minutes, "
                "LOWER(subType) as subType "
            f"FROM {creds['cb_bucket']}.._default.COMMON "
            "WHERE type='JOB' "
                "AND version='V01' "
                "AND status='active' "
            "ORDER BY offset_minutes, "
                    "run_priority "
        )
        # fmt: on

    def execute_query(query: str) -> list[JobRunCriterion]:
        """Executes the given query and returns the results"""
        row_iter = cluster.query(query, QueryOptions(read_only=True))  # type: ignore[assignment]
        return [row for row in row_iter]

    if job_id is not None:
        query = build_query_for_job_id(job_id)
    elif os.getenv("VXINGEST_IGNORE_JOB_SCHEDULE") == "true":
        query = build_query_for_active_jobs()
    else:
        query = build_query_for_scheduled_active_jobs()

    return execute_query(query)


def connect_cb(creds: dict[str, str]) -> Cluster:
    """
    Create a connection to the specified Couchbase cluster
    """
    auth = PasswordAuthenticator(creds["cb_user"], creds["cb_password"])

    timeout_config = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )

    # Get a reference to our cluster
    # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
    logger.info(f"Connecting to Couchbase at: {creds['cb_host']}")
    _attempts = 0
    while _attempts < 3:
        try:
            cluster = Cluster(
                f"{creds['cb_host']}",
                ClusterOptions(auth, timeout_options=timeout_config),  # type: ignore
            )
            break
        except CouchbaseException as _e:
            logger.error(
                f"Error connecting to Couchbase server at: {creds['cb_host']}. Exception: {_e}. Attempt {_attempts + 1} of 3"
            )
            time.sleep(5)
            _attempts = _attempts + 1
    if _attempts == 3:
        raise CouchbaseException(
            message="Could not connect to couchbase after 3 attempts"
        )

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))
    return cluster


def determine_num_processes() -> int:
    """Calculate the number of processes to use

    If we can determine the number of cores available to us, allow
    numCores - 2 processes. Otherwise, only allow one new process.
    """
    num_cpus = os.cpu_count()
    if num_cpus:
        # Leave 2 processes available for the OS & main program
        cpus = num_cpus - 2 if num_cpus > 2 else 1
        logger.info(f"Machine has {num_cpus} CPUs available, using {cpus} of them")
    else:  # Unable to determine number of cpus
        logger.warning(
            "Unable to determine number of CPUs, only spinning up 1 extra process"
        )
        cpus = 1
    return cpus


def make_tarfile(output_tarfile: Path, source_dir: Path):
    """Create a tarfile, with the source_dir as the root of the tarfile contents"""
    with tarfile.open(output_tarfile, "w:gz") as tar:
        tar.add(source_dir, arcname=Path(source_dir).name)


# docs are either the older job docs represented as <JobRunCriterion> or the newer processSpecification docs
# represented by processSpecId. The criterion has an id and some othre fields. The other
# fields are not used for the newer processSpecification docs, just the id is used.
def process_run_configurations(
    cluster: Cluster,
    job_run_criteria: list[JobRunCriterion],
    startime: datetime,
    args,
    log_configurer: Callable,
    log_queue: Queue,
    ql,
) -> None:
    """
    Parses the given job docs with the appropriate method
    Example job criteria (old style):
     [{'id': 'JOB-TEST:V01:METAR:NETCDF:OBS', 'name': 'job-test:v01:metar:netcdf:obs', 'offset_minutes': 0, 'run_priority': 2, 'subType': 'netcdf'}]

    Example runtime job_run_criteria:
    ['PS:METAR:NETCDF:OBS:MADIS-TEST:V01']
    """
    logger.info("Processing the job docs")
    # explicitly set the prometheus metrics to zero before starting (in multiple runs these can get confused)
    prom_successes._value.set(0)
    prom_failures._value.set(0)
    success_count = 0
    fail_count = 0
    runtime_collection = (
        cluster.bucket("vxdata").scope("_default").collection("RUNTIME")
    )
    common_collection = cluster.bucket("vxdata").scope("_default").collection("COMMON")
    for job in job_run_criteria:
        logger.info(f"Processing job: {job}")
        if job["id"].startswith("PS:"):
            # this is a newer runtime job document
            proc = runtime_collection.get(job["id"]).content_as[dict]
            job["sub_type"] = proc["subType"]
            # get config values from the runtime document heirachy for this process
            ingest_document_ids = proc["ingestDocumentIds"]
            data_source_id = proc["dataSourceId"]
            data_source_spec = runtime_collection.get(data_source_id).content_as[dict]
            input_data_path = data_source_spec["sourceDataUri"]
            file_mask = data_source_spec["fileMask"]
            collection = data_source_spec["subset"]
        else:
            proc = common_collection.get(job["id"]).content_as[dict]
            file_mask = proc["file_mask"]
            input_data_path = proc["input_data_path"]
            ingest_document_ids = proc["ingest_document_ids"]
            collection = proc.get("subset")
        name = proc["id"].replace("_", "__").replace(":", "_")

        # Add a logging file handler with a unique name for just this proc
        logpath = (
            args.log_dir / f"{name}-{startime.strftime('%Y-%m-%dT%H:%M:%S%z')}.log"
        )
        f_handler = add_logfile(ql, logpath)

        metric_name = f"{name}"
        logger.info(f"metric_name {metric_name}")

        # create an output directory with the time this proc was started.
        output_dir = (
            Path(args.output_dir)
            / f"{proc['subType']}_to_cb"
            / "output"
            / f"{startime.strftime('%Y%m%d%H%M%S')}"
        )
        create_dirs([output_dir])
        # Create the config dictionary for this job
        config = {
            "credentials_file": str(args.credentials_file),
            "collection": collection,
            "job_id": args.job_id,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": str(output_dir),
            "threads": args.threads,
            "first_epoch": args.start_epoch,  # TODO - this arg is only supported in the CTC & SUM builders
            "last_epoch": args.end_epoch,  # TODO - this arg is only supported in the CTC & SUM builders
            "file_pattern": args.file_pattern,  # TODO - this arg is only supported in the grib & netcdf builders
        }
        proc_succeeded = False
        match proc["subType"]:
            case "GRIB2" | "GRIB2-TEST":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    grib_ingest = GRIBIngest()
                    grib_ingest.runit(
                        config,
                        log_queue,
                        log_configurer,
                    )
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        proc_succeeded = True
                else:
                    proc_succeeded = True
            case "NETCDF" | "NETCDF-TEST":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    netcdf_ingest = NetCDFIngest()
                    netcdf_ingest.runit(
                        config,
                        log_queue,
                        log_configurer,
                    )
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        proc_succeeded = True
                else:
                    proc_succeeded = True
            case "CTC" | "CTC-TEST":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    ctc_ingest = CTCIngest()
                    ctc_ingest.runit(
                        config,
                        log_queue,
                        log_configurer,
                    )
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        proc_succeeded = True
                else:
                    proc_succeeded = True
            case "PARTIAL_SUMS" | "PARTIAL_SUMS-TEST":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    partial_sums_ingest = PartialSumsIngest()
                    partial_sums_ingest.runit(
                        config,
                        log_queue,
                        log_configurer,
                    )
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        proc_succeeded = True
                else:
                    proc_succeeded = True
            # case "PREPBUFR" | "PREPBUFR-TEST":
            #     # FIXME: Update calling code to raise instead of calling sys.exit
            #     try:
            #         prepbufr_ingest = PrepbufrIngest()
            #         prepbufr_ingest.runit(
            #             config,
            #             log_queue,
            #             log_configurer,
            #         )
            #     except SystemExit as e:
            #         if e.code == 0:
            #             # Job succeeded
            #             proc_succeeded = True
            #     else:
            #         proc_succeeded = True
            case _:
                logger.error(f"No ingest method for {proc['subType']}")
                proc_succeeded = False
        if proc_succeeded:
            success_count += 1
            # Update prometheus metrics
            prom_successes.inc()
            prom_last_success.set_to_current_time()
        else:
            fail_count += 1
            # Update prometheus metrics
            prom_failures.inc()
        logger.info(f"Done processing  proc: {proc}")
        logger.info(f"exit_code:{0 if proc_succeeded else 1}")
        # Remove the filehandler with the unique filename for this proc
        remove_logfile(f_handler, ql)
        # Move the logfile to the output dir
        logpath.rename(
            output_dir / f"{name}-{startime.strftime('%Y-%m-%dT%H:%M:%S%z')}.log"
        )
        # Create a tarfile and delete the output directory contents
        tar_filename = f"{metric_name}_{startime.strftime('%s')}.tar.gz"
        make_tarfile(args.transfer_dir / tar_filename, output_dir)
        logger.info(f"Created tarfile at: {args.transfer_dir / tar_filename}")
        logger.info(f"Removing: {output_dir}")
        shutil.rmtree(output_dir)
    logger.info(f"Success: {success_count}, Fail: {fail_count}")


def run_ingest() -> None:
    """entrypoint"""
    # Force new processes to start with a clean environment
    # "fork" is the default on Linux and can be unsafe
    if not hasattr(run_ingest, "_start_method_set"):
        with contextlib.suppress(RuntimeError):
            set_start_method("spawn")
        run_ingest._start_method_set = True

    args = process_cli()

    # Setup logging for the main process so we can use the "logger"
    log_queue = Queue()
    runtime = datetime.now()
    log_queue_listener = configure_logging(
        log_queue,
        args.log_dir / f"all_logs-{runtime.strftime('%Y-%m-%dT%H:%M:%S%z')}.log",
    )
    # set profiling output
    os.environ["PROFILE_OUTPUT_DIR"] = str(args.log_dir)

    logger.info("Getting credentials")
    creds = get_credentials(args.credentials_file)

    logger.info("Creating required directories")
    dirs = [args.metrics_dir, args.output_dir, args.log_dir, args.transfer_dir]
    create_dirs(dirs)

    logger.info("Connecting to Couchbase")
    try:
        cluster = connect_cb(creds)
    except (TimeoutException, CouchbaseException):
        logger.critical(
            f"Error connecting to Couchbase server at: {creds['cb_host']}.",
            exc_info=True,
        )
        sys.exit(1)

    logger.info("Getting proc docs")
    if args.job_id and args.job_id.startswith("JS:"):
        # this is a newer type of runtime job doc that doesn't have scheduling info
        # just retrieve it and process it, don't schedule it
        rt_job_doc = get_runtime_job_criteria(cluster, creds, args.job_id)
        # this is the new kind of process_specification doc
        # run_criteria is overloaded for the older and the newer job documents
        # for the older job docs the criteria is derived from the job doc itself
        # for the newer runtime job docs, the criteria just has the id of the process specification doc
        run_criteria = [{"id": proc_id} for proc_id in rt_job_doc["processSpecIds"]]
    else:
        # this is an older type of doc (a job doc) that has scheduling info
        run_criteria = get_older_job_doc_criteria(cluster, creds, args.job_id)
    if not run_criteria:
        logger.info("No proc docs found")
        sys.exit(0)
    logger.info(f"Found {len(run_criteria)} proc docs")
    logger.debug(f"Job docs to process: {run_criteria}")

    logger.info("Processing proc docs")
    process_run_configurations(
        cluster,
        run_criteria,
        runtime,
        args,
        worker_log_configurer,
        log_queue,
        log_queue_listener,
    )
    endtime = datetime.now()
    logger.info("Done processing proc docs")

    # Write prometheus metrics
    duration = endtime - runtime
    logger.info(f"Runtime was {duration.total_seconds()} seconds")
    prom_duration.set(duration.total_seconds())
    prom_file = (
        args.metrics_dir / "run_ingest_metrics.prom"
    )  # FIXME - should this be part of the tarball?
    logger.info(f"Writing Prometheus metrics to: {prom_file}")
    write_to_textfile(prom_file, prom_registry)

    # Tell the logging thread to finish up, too
    log_queue_listener.stop()
    logger.info("Done")


if __name__ == "__main__":
    run_ingest()
