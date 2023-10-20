# The entrypoint for VxIngest
# Responsible for checking the existing jobs documents and determining which need to be run

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import yaml
from couchbase.auth import PasswordAuthenticator  # type: ignore
from couchbase.cluster import Cluster  # type: ignore
from couchbase.exceptions import CouchbaseException, TimeoutException  # type: ignore
from couchbase.options import (  # type: ignore
    ClusterOptions,
    ClusterTimeoutOptions,
    QueryOptions,
)

import ctc_to_cb.run_ingest_threads
import grib2_to_cb.run_ingest_threads
import netcdf_to_cb.run_ingest_threads

# Logger setup
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",  # ISO 8601
)
logger = logging.getLogger(__name__)


def get_credentials(path: str) -> Dict[str, str]:
    """
    Loads a YAML config file from the given path.

    Returns a dictionary of values from the file
    """

    # Check the file exists
    if not Path(path).is_file():
        raise FileNotFoundError(f"Credentials file can not be found: {path}")

    # Load the file
    config: Dict[str, str] = {}
    with open(path, encoding="utf-8") as file:
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


def process_cli():
    """Processes the following flags

    -c - credentials file path
    -m - metrics directory path
    -o - output directory path
    -x - "transfer" directory path
    -l - log directory path
    -j - jobid (optional)
    -s - start epoch (optional?)
    -e - end epoch (optional?)
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
        type=str,
        required=False,
        default="config.yaml",
        help="Path to the credentials file",
    )
    parser.add_argument(
        "-m",
        "--metrics_dir",
        type=str,
        required=True,
        help="Path to write metrics files",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        required=True,
        help="Path to write output couchbase JSON files",
    )
    parser.add_argument(
        "-x",
        "--transfer_dir",
        type=str,
        required=True,
        help="Path to the 'transfer directory'",
    )
    parser.add_argument(
        "-l",
        "--log_dir",
        type=str,
        required=True,
        help="Path to write log files",
    )
    parser.add_argument(
        "-s",
        "--start_epoch",
        type=int,
        required=False,
        default=0,
        help="The first epoch to process jobs for, inclusive",
    )
    parser.add_argument(
        "-e",
        "--end_epoch",
        type=int,
        required=False,
        default=sys.maxsize,
        help="The last epoch to process jobs for, exclusive",
    )
    # get the command line arguments
    args = parser.parse_args()
    return args


def create_dirs(paths: list[str] | list[Path]) -> None:
    """Creates directory structure"""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def get_job_docs(
    cluster,  #: Cluster.bucket.Collection,
    creds: Dict[str, str],
    job_id: Optional[str] = None,
) -> list[dict]:
    """Queries Couchbase for the given job doc or job docs in need of processing if no job ID is given"""
    
    # TODO - We're doing this query at the cluster level. Would it be better to query at the scope level if we're using Couchbase 7?
    # https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html#querying-at-scope-level
    if job_id is not None:
        # fmt: off
        # Disable Black to keep the Couchbase query readable
        query = (
            "SELECT meta().id AS id, "
                "LOWER(META().id) as name, "
                "run_priority, "
                "offset_minutes, "
                "LOWER(subType) as sub_type, "
                "input_data_path as input_data_path "
            f"FROM {creds['cb_bucket']}.{creds['cb_scope']}.{creds['cb_collection']} "
            f"WHERE id='{job_id}' "
                "AND type = 'JOB-TEST' " # FIXME: Should this be a JOB type like below?
                # "AND type = 'JOB' "
                "AND version = 'V01' "
                "AND CONTAINS(status, 'active') "
        )
        # fmt: on
        row_iter = cluster.query(query, QueryOptions(read_only=True))
        return [row for row in row_iter]

    # fmt: off
    # Disable Black to keep the Couchbase query readable
    query = (
        "SELECT meta().id AS id, "
            "LOWER(META().id) as name, "
            "run_priority, "
            "offset_minutes, "
            "LOWER(subType) as sub_type, "
            "input_data_path as input_data_path "
        f"FROM {creds['cb_bucket']}.{creds['cb_scope']}.{creds['cb_collection']} "
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
    row_iter = cluster.query(query, QueryOptions(read_only=True))
    return [row for row in row_iter]


def connect_cb(creds: Dict[str, str]) -> Cluster:
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
    cluster = Cluster(
        f"couchbase://{creds['cb_host']}",
        ClusterOptions(auth, timeout_options=timeout_config),  # type: ignore
    )

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))

    # Set the cluster to use the correct bucket and collection
    # TODO - is this needed? The couchbase docs seemed to indicate it was
    bucket = cluster.bucket(creds["cb_bucket"])
    collection = bucket.scope(creds["cb_scope"]).collection(creds["cb_collection"])

    return cluster


def process_jobs(job_docs, starttime, args):
    """
    Parses the given job docs and determines which method to ingest them with

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
    for job in job_docs:
        logger.info(f"Processing job: {job}")
        # translate _ to __ and : to _ for the name field
        # TODO - why is this needed?
        name = job["name"].replace("_", "__").replace(":", "_")

        # create an output_dir/(sub_type field)_to_cb/output/$(date +%Y%m%d%H%M%S) directory
        # output_dir = Path("output_dir") / f"{job['sub_type']}_to_cb" / "output" / f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
        # create a unique output_dir for this job so we can run multiple jobs
        output_dir = (
            Path(args.output_dir)
            / f"{job['sub_type']}_to_cb"
            / "output"
            / f"{uuid.uuid4()}"
        )
        create_dirs([output_dir])
        logfile = Path(args.log_dir) / f"{name}-{starttime}.log"
        # TODO: create a log_dir/$(name)-$(runtime).log file

        # TODO: create a metric_name  - $(name)_$(hostname | tr '-' '_') & insert into logfile

        config = {
            "job_id": job["id"],
            "credentials_file": args.credentials_file,
            "output_dir": str(output_dir),
            "threads": 8,
        }
        job_succeeded = False
        match job["sub_type"]:
            case "grib2":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    ingest = grib2_to_cb.run_ingest_threads.VXIngest()
                    ingest.runit(config)
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        job_succeeded = True
                else:
                    job_succeeded = True
            case "netcdf":
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    ingest = netcdf_to_cb.run_ingest_threads.VXIngest()
                    ingest.runit(config)
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        job_succeeded = True
                else:
                    job_succeeded = True
            case "ctc":
                # FIXME: We need to override the config as CTCs are currently inconsistent and take extra values
                config = {
                    "job_id": job["id"],
                    "credentials_file": args.credentials_file,
                    "output_dir": str(output_dir),
                    "threads": 8,  # TODO - does this need to thread on the for loop?
                    "first_epoch": args.start_epoch,  # TODO - this is inconsistent - only for CTCs - default is 0 - flag is "-f"
                    "last_epoch": args.end_epoch,  # TODO - this is inconsistenet - only for CTCs - default is sys.maxsize - flag is "-l"
                }
                # FIXME: Update calling code to raise instead of calling sys.exit
                try:
                    ingest = ctc_to_cb.run_ingest_threads.VXIngest()
                    ingest.runit(config)
                except SystemExit as e:
                    if e.code == 0:
                        # Job succeeded
                        job_succeeded = True
                else:
                    job_succeeded = True

        if job_succeeded:
            success_count += 1
        else:
            fail_count += 1

    logger.info(f"Success: {success_count}, Fail: {fail_count}")
    # TODO: move things to output_dir, tar it into the xfer_dir, then prune output_dir if empty
    # TODO: make a prom metrics file with same metrics that run_ingest.sh emits # TODO - use the prometheus client & the write_to_textfile writer


def run_ingest():
    """entrypoint"""
    args = process_cli()
    creds = get_credentials(args.credentials_file)

    dirs = [args.metrics_dir, args.output_dir, args.log_dir, args.transfer_dir]
    logger.info("Creating required directories")
    create_dirs(dirs)

    try:
        logger.info("Connecting to Couchbase")
        cluster = connect_cb(creds)
    except (TimeoutException, CouchbaseException) as e:
        logger.fatal(f"Error connecting to Couchbase: {e}")
        sys.exit(1)

    logger.info("Getting job docs")
    docs = get_job_docs(cluster, creds, args.job_id)
    if not docs:
        logger.info("No job docs found")
        sys.exit(0)
    logger.info(f"Found {len(docs)} job docs")
    logger.debug(f"Job docs to process: {docs}")

    logger.info("Processing job docs")
    runtime = datetime.now()
    process_jobs(docs, runtime, args)
    logger.info("Done processing job docs")


if __name__ == "__main__":
    run_ingest()
