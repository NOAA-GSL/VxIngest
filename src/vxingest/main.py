# The entrypoint for VxIngest
# Responsible for checking the existing jobs documents and determining which need to be run

import argparse
import logging
import os
import shutil
import sys
import tarfile
import time
from datetime import datetime, timedelta
from multiprocessing import Queue, set_start_method
from pathlib import Path
from typing import Callable, Optional, TypedDict

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


class processDoc(TypedDict):
    """A type class describing the expected return result from Couchbase"""

    id: str
    name: str
    offset_minutes: int
    run_priority: int
    sub_type: str

def get_runtime_job_doc(
    cluster: Cluster,
    creds: dict[str, str],
    job_id: Optional[str] = None,
) -> processDoc | None:
    """
    Queries the Couchbase database for a specific job document by its ID.
    This is used to retrieve the newer runtime job documents type "JS" for processing.

    Args:
        cluster (Cluster): The Couchbase cluster instance to use for querying.
        creds (dict[str, str]): A dictionary containing Couchbase credentials, including 'cb_bucket' and 'cb_scope'.
        job_id (Optional[str], optional): The ID of the job document to retrieve. Must be provided.

    Returns:
        processDoc | None: The job document if found, otherwise None.

    Raises:
        ValueError: If job_id is not provided.
    """
    if not job_id:
        raise ValueError("job_id must be provided to get_runtime_job_doc")

    # Build the query to fetch a specific job document by ID
    query = (
        "SELECT meta().id AS id, "
        "LOWER(META().id) as name, "
        "subType",
        "subset",
        "processSpecIds",
        "status"
        f"FROM {creds['cb_bucket']}._default.RUNTIME"
        f"WHERE id='{job_id}' "
            "AND type = 'JS' "
            "AND version = 'V01' "
            "AND (status = 'active'  OR status = 'test') "
    )

    row_iter = cluster.query(query, QueryOptions(read_only=True))  # type: ignore[assignment]
    for row in row_iter:
        return row
    return None

def get_job_docs(
    cluster: Cluster,
    creds: dict[str, str],
    job_id: Optional[str] = None,
) -> list[processDoc]:
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
        job_id (Optional[str], optional): The ID of a specific job document to fetch. If not provided, fetches jobs based on schedule and status.

    Returns:
        list[processDoc]: A list of job documents matching the query criteria.
    """
    """Queries Couchbase for the given job doc or job docs in need of processing if no job ID is given"""

    # TODO - We're doing this query at the cluster level. Would it be better to query at the scope level if we're using Couchbase 7?
    # https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html#querying-at-scope-level

    def build_query_for_job_id(job_id: str) -> str:
        """Builds a query to fetch a specific job document by ID"""
        # fmt: off
        # Disable formatting to keep the Couchbase query readable
        # the older job documents are kept in the COMMON collection along with other metadata.
        # Eventually they will be replaced by newer JS documents that are in the RUNTIM collection
        # and deleted entirely.
        # TODO remove JOB documents from the COMMON collection as they are superceded by JS documents
        # in the RUNTIME collection
        return (
            "SELECT meta().id AS id, "
                "LOWER(META().id) as name, "
                "run_priority, "
                "offset_minutes, "
                "LOWER(subType) as sub_type "
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
                "LOWER(subType) as sub_type "
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
                "LOWER(subType) as sub_type "
            f"FROM {creds['cb_bucket']}.._default.COMMON "
            "WHERE type='JOB' "
                "AND version='V01' "
                "AND status='active' "
            "ORDER BY offset_minutes, "
                    "run_priority "
        )
        # fmt: on

    def execute_query(query: str) -> list[processDoc]:
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


def process_docs(
    cluster: Cluster,
    process_docs: list[processDoc],
    startime: datetime,
    args,
    log_configurer: Callable,
    log_queue: Queue,
    ql,
) -> None:
    """
    Parses the given job docs with the appropriate method

    Test Job IDs:
    [
        {"id": "JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS"},
        {"id": "JOB-TEST:V01:METAR:CTC:VISIBILITY:MODEL:OPS"},
        {"id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"},
        {"id": "JOB-TEST:V01:METAR:GRIB2:MODEL:RAP_OPS_130"},
        {"id": "JOB-TEST:V01:METAR:NETCDF:OBS"}
    ]

    Example job doc:
    {
        'id': 'JOB:V01:METAR:CTC:CEILING:MODEL:HRRR_RAP_130',
        'name': 'job:v01:metar:ctc:ceiling:model:hrrr_rap_130',
        'offset_minutes': 0,
        'run_priority': 6,
        'sub_type': 'ctc'
    }
    """
    logger.info("Processing the job docs")

    success_count = 0
    fail_count = 0
    for proc in process_docs:
        logger.info(f"Processing job: {proc}")
        # translate _ to __ and : to _ for the name field
        # TODO - why is this needed?
        name = proc["name"].replace("_", "__").replace(":", "_")

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
            / f"{proc['sub_type']}_to_cb"
            / "output"
            / f"{startime.strftime('%Y%m%d%H%M%S')}"
        )
        create_dirs([output_dir])

        # get config values from the runtime document heirachy for this process
        runtime_collection = cluster.bucket("vxdata").scope("_default").collection("RUNTIME")
        job_spec = runtime_collection.get("JS:METAR:OBS:NETCDF:schedule:job:V01").content_as[
            dict
        ]
        process_id = job_spec["processSpecIds"][0]
        process_spec = runtime_collection.get(process_id).content_as[dict]
        ingest_document_ids = process_spec["ingestDocumentIds"]
        data_source_id = process_spec["dataSourceId"]
        data_source_spec = runtime_collection.get(data_source_id).content_as[dict]
        collection = process_spec["subset"]
        input_data_path = data_source_spec["sourceDataUri"]
        file_mask = data_source_spec["fileMask"]
        ingest_document_ids = []

        # Create the config dictionary for this job
        config = (
            {
                "credentials_file": str(args.credentials_file),
                "collection": collection,
                "file_mask": file_mask,
                "input_data_path": input_data_path,
                "ingest_document_ids": ingest_document_ids,
                "output_dir": str(output_dir),
                "threads": args.threads,
                "first_epoch": args.start_epoch,  # TODO - this arg is only supported in the CTC & SUM builders
                "last_epoch": args.end_epoch,  # TODO - this arg is only supported in the CTC & SUM builders
                "file_pattern": args.file_pattern,  # TODO - this arg is only supported in the grib & netcdf builders
            },
        )
        proc_succeeded = False
        match proc["sub_type"]:
            case "grib2":
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
            case "netcdf":
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
            case "ctc":
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
            case "partial_sums":
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
            case _:
                logger.error(f"No ingest method for {proc['sub_type']}")
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
    set_start_method("spawn")

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
        # this is a newer type of proc doc that doesn't have scheduling info
        # just retrieve it and process it, don't schedule it
        job_doc = get_runtime_job_doc(cluster, creds, args.job_id)
        docs = job_doc["processSpecIds"]
    else:
        # this is an older type of proc doc that has scheduling info
        docs = get_job_docs(cluster, creds, args.job_id)
    if not docs:
        logger.info("No proc docs found")
        sys.exit(0)
    logger.info(f"Found {len(docs)} proc docs")
    logger.debug(f"Job docs to process: {docs}")

    logger.info("Processing proc docs")
    process_docs(
        cluster,
        docs,
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
