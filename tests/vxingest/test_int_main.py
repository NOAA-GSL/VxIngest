import json
import math
import os
import sys
import tarfile
from pathlib import Path

import pytest
from couchbase.options import QueryOptions

# from vxingest.ctc_to_cb.run_ingest_threads import VXIngest_ctc
from vxingest.grib2_to_cb.run_ingest_threads import VXIngest as VXIngest_grib2
from vxingest.main import run_ingest
from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest as VXIngest_netcdf

# from vxingest.partial_sums_to_cb.run_ingest_threads import VXIngest_partial_sums


def setup_connection(_vx_ingest):
    """test setup"""
    # Ensure credentials_file is a string, not a tuple
    credentials = os.environ["CREDENTIALS"]
    if isinstance(credentials, tuple):
        credentials = credentials[0]
    _vx_ingest.credentials_file = credentials
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    try:
        id_query = """DELETE
                FROM `vxdata`.`_default`.`METAR` f
                WHERE f.subset = 'METAR'
                AND f.type = 'DF'
                AND f.url LIKE '/opt/data/%' RETURNING f.id AS id;"""
        row_iter = _vx_ingest.cluster.query(
            id_query, QueryOptions(metrics=True, read_only=False)
        )
        for row in row_iter:
            print(f"Deleted {row['id']}")
    except Exception as e:
        pytest.fail(f"Error during setup connection: {e}")
    return _vx_ingest


def assert_dicts_almost_equal(dict1, dict2, rel_tol=1e-09):
    """Utility function to compare potentially nested dictionaries containing floats"""
    assert set(dict1.keys()) == set(dict2.keys()), (
        f"Dictionaries do not have the same keys {dict1.keys()} vs {dict2.keys()}"
    )
    for key in dict1:
        if isinstance(dict1[key], dict):
            assert_dicts_almost_equal(dict1[key], dict2[key], rel_tol)
        else:
            assert dict1[key] == pytest.approx(dict2[key], rel=rel_tol), (
                f"Values for station {dict1['name']} {key} do not match dict1[key]: {dict1[key]} dict2[key]: {dict2[key]}"
            )


@pytest.mark.integration
def test_one_thread_specify_file_pattern_netcdf_job_spec_rt(tmp_path: Path):
    # Save original sys.argv
    original_argv = sys.argv.copy()
    job_id = "JS:METAR:OBS:NETCDF-TEST:schedule:job:V01"
    # need these args
    sys.argv = [
        "run_ingest",
        "-j",
        job_id,
        "-c",
        os.environ["CREDENTIALS"],
        "-m",
        str(tmp_path / "metrics"),
        "-o",
        str(tmp_path / "output"),
        "-x",
        str(tmp_path / "transfer"),
        "-l",
        str(tmp_path / "logs"),
        "-f",
        "[0123456789]???????_[0123456789]???",
        "-t",
        "1",
    ]
    try:
        vx_ingest = setup_connection(VXIngest_netcdf())
        run_ingest()
        check_output(tmp_path, vx_ingest, 6)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


@pytest.mark.integration
def test_one_thread_specify_file_pattern_netcdf_job_spec_type_job(tmp_path: Path):
    # Save original sys.argv
    original_argv = sys.argv.copy()
    job_id = "JOB-TEST:V01:METAR:NETCDF:OBS"
    # need these args
    sys.argv = [
        "run_ingest",
        "-j",
        job_id,
        "-c",
        os.environ["CREDENTIALS"],
        "-m",
        str(tmp_path / "metrics"),
        "-o",
        str(tmp_path / "output"),
        "-x",
        str(tmp_path / "transfer"),
        "-l",
        str(tmp_path / "logs"),
        "-f",
        "[0123456789]???????_[0123456789]???",
        "-t",
        "1",
    ]
    try:
        vx_ingest = setup_connection(VXIngest_netcdf())
        run_ingest()
        check_output(tmp_path, vx_ingest, 6)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


@pytest.mark.integration
def test_one_thread_specify_file_pattern_grib2_job_spec_rt(tmp_path: Path):
    # Save original sys.argv
    original_argv = sys.argv.copy()
    job_id = "JS:METAR:OBS:GRIB2-TEST:schedule:job:V01"
    # need these args
    sys.argv = [
        "run_ingest",
        "-j",
        job_id,
        "-c",
        os.environ["CREDENTIALS"],
        "-m",
        str(tmp_path / "metrics"),
        "-o",
        str(tmp_path / "output"),
        "-x",
        str(tmp_path / "transfer"),
        "-l",
        str(tmp_path / "logs"),
        "-f",
        "[0123456789]???????_[0123456789]???",
        "-t",
        "1",
    ]
    try:
        vx_ingest = setup_connection(VXIngest_grib2())
        run_ingest()
        check_output(tmp_path, vx_ingest, 6)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


@pytest.mark.integration
def test_one_thread_specify_file_pattern_grib2_job_spec_type_job(tmp_path: Path):
    # Save original sys.argv
    original_argv = sys.argv.copy()
    job_id = "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
    # need these args
    sys.argv = [
        "run_ingest",
        "-j",
        job_id,
        "-c",
        os.environ["CREDENTIALS"],
        "-m",
        str(tmp_path / "metrics"),
        "-o",
        str(tmp_path / "output"),
        "-x",
        str(tmp_path / "transfer"),
        "-l",
        str(tmp_path / "logs"),
        "-f",
        "21287230000[0123456789]?",
        "-t",
        "1",
    ]
    try:
        vx_ingest = setup_connection(VXIngest_grib2())
        run_ingest()
        # NOTE: only 3 files match the pattern in this job
        check_output(tmp_path, vx_ingest, 3)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


def check_output(tmp_path, vx_ingest, file_count):
    # do the output checking here
    # something like this:
    # ├── logs
    # │   └── all_logs-2025-10-27T12:26:06.log
    # ├── metrics
    # │   └── run_ingest_metrics.prom
    # ├── output
    # │   └── NETCDF_to_cb
    # │       └── output
    # ├── results
    # │   └── 20251027122606
    # │       ├── 20210920_1700.json
    # │       ├── 20211105_0600.json
    # │       ├── 20211105_2300.json
    # │       ├── 20211108_0000.json
    # │       ├── 20211130_1000.json
    # │       ├── 20250911_1500.json
    # │       ├── JOB-TEST_V01_METAR_NETCDF_OBS-2025-10-27T12:26:06.log
    # │       └── LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:1761589599.json
    # └── transfer
    #     └── JOB-TEST_V01_METAR_NETCDF_OBS_1761589566.tar.gz
    try:
        # do the directories exist?
        assert (tmp_path / "output").exists()
        # is the output directory empty?
        assert any((tmp_path / "output").iterdir())
        # does the logs dir exist?
        assert (tmp_path / "logs").exists()
        # are there any errors in the all_logs file?
        all_logs_files = list((tmp_path / "logs").glob("all_logs-*.log"))
        assert len(all_logs_files) == 1
        all_logs_file = all_logs_files[0]
        with all_logs_file.open("r") as f:
            all_logs_content = f.read()
            assert (
                "error" not in all_logs_content.lower()
            )  # are there the expected number of log files?
        assert (tmp_path / "metrics").exists()
        metrics_files = list((tmp_path / "metrics").glob("*.prom"))
        assert len(metrics_files) == 1
        metrics_file = metrics_files[0]
        with metrics_file.open("r") as f:
            metrics_content = f.read()
            assert "run_ingest_success_count_total 1.0" in metrics_content
            assert "run_ingest_failure_count_total 0.0" in metrics_content
        assert (tmp_path / "transfer").exists()
        transfer_files = list((tmp_path / "transfer").glob("*.tar.gz"))
        assert len(transfer_files) == 1
        # check the contents of the tar.gz file
        tarFile = transfer_files[0]
        (tmp_path / "results").mkdir()
        with tarfile.open(tarFile, "r:gz") as tar:
            # List all files in the archive
            names = tar.getnames()
            numFiles = len(names)
            assert (
                numFiles == file_count + 3
            )  # including log file, LJ file, and directory
            # Extract all files to the results directory
            tar.extractall(path=tmp_path / "results")
            # get one of the json files and check its contents
            json_files = [n for n in names if n.endswith(".json") and "LJ:" not in n]
            assert len(json_files) == file_count
            sample_json_file = json_files[0]
            with (tmp_path / "results" / sample_json_file).open("r") as jf:
                derived_data = json.load(jf)
                for document in derived_data:
                    if document["id"].startswith("LJ"):
                        check_load_job(document)
                        continue
                    if "DD" in document["id"] and document["docType"] == "obs":
                        check_netcdf(vx_ingest, document)
                        break
                    if "DD" in document["id"] and document["docType"] == "model":
                        check_grib2(vx_ingest, document)
                        break
    except Exception as exc:
        pytest.fail(f"Exception occurred: {exc}")


def check_load_job(derived_data):
    _id = derived_data["id"]
    if _id.startswith("LJ"):
        for _k in derived_data:
            assert _k in [
                "id",
                "subset",
                "type",
                "lineageId",
                "script",
                "scriptVersion",
                "loadSpec",
                "note",
            ], (
                f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus LJ failure key {_k} not in {derived_data.keys()}"
            )


def check_grib2(vx_ingest, derived_data):
    _id = derived_data["id"]
    _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
    _qresult = vx_ingest.cluster.query(_statement)
    result_rows = list(_qresult.rows())
    assert len(result_rows) > 0, (
        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure test document {_id} not found in couchbase"
    )

    result = result_rows[0]
    # assert top level fields
    keys = derived_data.keys()
    for _k in result:
        assert _k in keys, (
            f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure top level key {_k} not in {derived_data.keys()}"
        )
    # assert the units
    assert result["units"] == derived_data["units"], (
        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure units {result['units']} != {derived_data['units']}"
    )
    # assert the data
    for _k in result["data"]:
        assert _k in derived_data["data"], (
            f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k} not in {derived_data['data'].keys()}"
        )
        for _dk in result["data"][_k]:
            assert _dk in derived_data["data"][_k], (
                f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k}.{_dk} not in {derived_data['data'][_k].keys()}"
            )
            # assert data field matches to 2 decimal places
            if _dk == "name" or _dk == "Vegetation Type":
                # string compare
                assert result["data"][_k][_dk] == derived_data["data"][_k][_dk], (
                    f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure name {result['data'][_k][_dk]} != {derived_data['data'][_k][_dk]}"
                )

            else:
                # math compare
                # print(f"result {_k} {_dk} ", result["data"][_k][_dk])
                abs_tol = 0.0
                if _dk == "Ceiling":
                    abs_tol = 0.002  # ceiling values don't always have four decimals of resolution
                elif _dk == "DewPoint":
                    abs_tol = 1.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                    # abs_tol = 0.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                elif (
                    _dk == "RH"
                ):  # RH only has one decimal of resolution from the grib file
                    abs_tol = 1.00001  # not really sure why math.isclose compares out to 5 places but not 6
                    # abs_tol = 0.00001  # not really sure why math.isclose compares out to 5 places but not 6
                    # There are no unusual math transformations in the RH handler.
                else:
                    abs_tol = 0.001  # most fields validate between pygrib and cfgrib precisely

                assert result["data"][_k][_dk] is not None, (
                    f"""result {_k + "." + _dk}  is None """
                )
                assert derived_data["data"][_k][_dk] is not None, (
                    f"""derived_data {_k + "." + _dk} is None """
                )
                # Only compare with math.isclose if both are numbers
                if isinstance(result["data"][_k][_dk], (int, float)) and isinstance(
                    derived_data["data"][_k][_dk], (int, float)
                ):
                    assert math.isclose(
                        result["data"][_k][_dk],
                        derived_data["data"][_k][_dk],
                        abs_tol=abs_tol,
                    ), f"""TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data not close within {abs_tol}
                        {_k}.{_dk} {result["data"][_k][_dk]} != {derived_data["data"][_k][_dk]} within {abs_tol} decimal places."""
                else:
                    assert result["data"][_k][_dk] == derived_data["data"][_k][_dk], (
                        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure non-numeric data {result['data'][_k][_dk]} != {derived_data['data'][_k][_dk]}"
                    )


def check_netcdf(vx_ingest, derived_data):
    station_id = ""
    derived_station = {}
    obs_id = ""
    derived_obs = {}
    for item in derived_data:
        if item["docType"] == "station" and item["name"] == "KDEN":
            station_id = item["id"]
            derived_station = item
        else:
            if item["docType"] == "obs":
                obs_id = item["id"]
                derived_obs = item
        if derived_station and derived_obs:
            break
    retrieved_station = vx_ingest.collection.get(station_id).content_as[dict]
    retrieved_obs = vx_ingest.collection.get(obs_id).content_as[dict]
    # make sure the updateTime is the same in both the derived and retrieved station
    retrieved_station["updateTime"] = derived_station["updateTime"]
    # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
    retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][0]["firstTime"]
    retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0]["lastTime"]

    assert derived_station == retrieved_station
    assert_dicts_almost_equal(derived_obs, retrieved_obs)
