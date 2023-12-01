import tarfile
from pathlib import Path
from unittest import mock

import pytest
import yaml
from couchbase.cluster import Cluster

from vxingest.main import (
    create_dirs,
    determine_num_processes,
    get_credentials,
    get_job_docs,
    make_tarfile,
)


def test_get_credentials(tmp_path):
    data = {
        "cb_host": "localhost",
        "cb_user": "user",
        "cb_password": "password",
        "cb_bucket": "bucket",
        "cb_scope": "scope",
        "cb_collection": "collection",
    }
    file = Path(tmp_path) / "config.yaml"
    with file.open("w") as f:
        yaml.dump(data, f)

    assert get_credentials(file) == data


def test_get_credentials_missing_key(tmp_path):
    data = {
        "cb_host": "localhost",
        "cb_user": "user",
        "cb_password": "password",
        "cb_bucket": "bucket",
        "cb_scope": "scope",
        # "cb_collection": "collection",  # This key is missing
    }
    file = Path(tmp_path) / "config.yaml"
    with file.open("w") as f:
        yaml.dump(data, f)

    with pytest.raises(KeyError):
        get_credentials(file)


def test_create_dirs(tmp_path):
    dirs = [tmp_path / "dir1", tmp_path / "dir2", tmp_path / "dir3"]
    create_dirs(dirs)
    for dir in dirs:
        assert dir.exists()


@pytest.fixture()
def mock_cluster():
    """Test fixture to create a mock Couchbase Cluster object instance with a known return value"""
    mock_cluster = mock.create_autospec(Cluster, instance=True)
    mock_cluster.query.return_value = [
        {
            "id": "job1",
            "name": "job1",
            "run_priority": 1,
            "offset_minutes": 0,
            "sub_type": "type1",
            "input_data_path": "/path/to/data",
        }
    ]
    return mock_cluster


# FIXME: Explore less brittle approaches for these tests
def test_get_job_docs_with_job_id(mock_cluster):
    creds = {"cb_bucket": "bucket", "cb_scope": "scope", "cb_collection": "collection"}
    job_id = "job1"
    result = get_job_docs(mock_cluster, creds, job_id)
    assert result == mock_cluster.query("fake_query")


# FIXME: Explore less brittle approaches for these tests
def test_get_job_docs_without_job_id(mock_cluster):
    creds = {"cb_bucket": "bucket", "cb_scope": "scope", "cb_collection": "collection"}
    result = get_job_docs(mock_cluster, creds)
    assert result == mock_cluster.query("fake_query")


@pytest.mark.parametrize(
    ("cpu_count", "expected"),
    [
        (7, 5),
        (4, 2),
        (2, 1),
        (None, 1),
    ],
)
def test_determine_num_processes(cpu_count, expected):
    with mock.patch("os.cpu_count", return_value=cpu_count):
        assert determine_num_processes() == expected


def test_make_tarfile(tmp_path):
    # Create some files in the temporary directory
    for i in range(3):
        with Path(tmp_path / f"file{i}.txt").open("w") as f:
            f.write(f"This is file {i}")

    # Call make_tarfile to create a tarfile of the temporary directory
    output_tarfile = tmp_path / "output.tar.gz"
    make_tarfile(output_tarfile, tmp_path)

    # Open the tarfile and check that it contains the expected files
    with tarfile.open(output_tarfile, "r:gz") as tar:
        names = tar.getnames()
        assert len(names) == 4  # 3 files + the directory itself
        assert f"{tmp_path.name}/file0.txt" in names
        assert f"{tmp_path.name}/file1.txt" in names
        assert f"{tmp_path.name}/file2.txt" in names
