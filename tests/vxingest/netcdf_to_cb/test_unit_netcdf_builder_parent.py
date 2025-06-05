import unittest
from unittest.mock import MagicMock, patch

import pytest

from vxingest.netcdf_to_cb.netcdf_builder_parent import NetcdfBuilder


class TestNetcdfBuilder(unittest.TestCase):
    def setUp(self):
        """Set up a mock NetcdfBuilder instance for testing."""
        load_spec = {
            "cb_connection": {
                "bucket": "test_bucket",
                "scope": "test_scope",
                "collection": "test_collection",
                "common_collection": "COMMON",
            },
            "load_job_doc": {"id": "test_job_id"},
        }
        ingest_document = {
            "template": {"subset": "test_subset", "id": "*test_id"},
        }
        self.builder = NetcdfBuilder(load_spec, ingest_document)
        self.builder.ncdf_data_set = MagicMock()

    def test_get_database_connection_details(self):
        """Test retrieving database connection details."""
        queue_element = "test_file.nc"
        bucket, scope, collection, common_collection = (
            self.builder.get_database_connection_details(queue_element)
        )
        assert bucket == "test_bucket"
        assert scope == "test_scope"
        assert collection == "test_collection"
        assert common_collection == "COMMON"
        assert self.builder.file_name == "test_file.nc"

    def test_build_document_map_type_checks(self):
        """Test type checks in build_document_map."""
        with pytest.raises(TypeError):
            self.builder.build_document_map(123, "base_var_name")
        with pytest.raises(TypeError):
            self.builder.build_document_map("queue_element", 123)
        with pytest.raises(TypeError):
            self.builder.build_document_map("queue_element", "base_var_name", 123)

    @patch("vxingest.netcdf_to_cb.netcdf_builder_parent.cProfile.Profile")
    @patch("vxingest.netcdf_to_cb.netcdf_builder_parent.Path.open")
    def test_build_document_map_profiling(self, mock_open, mock_profile):
        """Test build_document_map with profiling enabled."""
        self.builder.do_profiling = True
        self.builder.handle_document = MagicMock()
        self.builder.get_document_map = MagicMock(return_value={})
        self.builder.create_data_file_id = MagicMock(return_value="test_data_file_id")
        self.builder.build_datafile_doc = MagicMock(return_value={"id": "test_id"})

        document_map = self.builder.build_document_map("queue_element", "base_var_name")
        self.builder.handle_document.assert_called_once_with("base_var_name")
        assert "test_id" in document_map

    def test_derive_id(self):
        """Test deriving an ID from a template."""
        self.builder.handle_named_function = MagicMock(return_value="function_value")
        self.builder.translate_template_item = MagicMock(return_value="item_value")

        template_id = "&function|param1:param2:*item"
        result = self.builder.derive_id(template_id=template_id, base_var_index=0)
        assert result == "function_value:param2:item_value"

    def test_translate_template_item(self):
        """Test translating a template item."""
        self.builder.ncdf_data_set.variables = {
            "test_var": MagicMock(return_value="test_value1")
        }
        result = self.builder.translate_template_item("*test_var", 0)
        # don't know how to properly mock the ncdf_data_set.variables
        assert result is not None

    def test_handle_document_type_check(self):
        """Test type check in handle_document."""
        with pytest.raises(TypeError):
            self.builder.handle_document(123)

    def test_build_datafile_doc(self):
        """Test building a datafile document."""
        file_name = "test_file.nc"
        data_file_id = "test_data_file_id"
        origin_type = "test_origin"
        with patch(
            "vxingest.netcdf_to_cb.netcdf_builder_parent.Path.stat"
        ) as mock_stat:
            mock_stat.return_value.st_mtime = 1234567890
            result = self.builder.build_datafile_doc(
                file_name, data_file_id, origin_type
            )
        assert result["id"] == data_file_id
        assert result["mtime"] == 1234567890
        assert result["subset"] == "test_subset"
        assert result["type"] == "DF"
        assert result["fileType"] == "netcdf"
        assert result["originType"] == origin_type

    def test_handle_named_function(self):
        """Test handling a named function."""
        self.builder.meterspersecond_to_milesperhour = MagicMock(return_value=10)
        result = self.builder.handle_named_function(
            "&meterspersecond_to_milesperhour|*speed", 0
        )
        assert result == 10

    def test_load_data(self):
        """Test loading data into a document."""
        doc = {"data": {}}
        element = {"name": "station1", "Reported Time": 1234567890}
        result = self.builder.load_data(doc, element)
        assert "station1" in result["data"]
        assert result["data"]["station1"] == element
