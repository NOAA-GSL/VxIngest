"""
Builder - Parent class for all Builders
"""

import logging
import os


class Builder:  # pylint: disable=too-many-arguments disable=too-many-instance-attributes
    """
    Parent class for all Builders
    """

    def __init__(self, load_spec, ingest_document):
        self.ingest_document = ingest_document
        # CTC builders cannot init the ingest_document or the template, they get set in the build_document
        self.template = None if ingest_document is None else ingest_document["template"]
        self.load_spec = load_spec
        self.an_id = None
        self.document_map = {}
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False

    def initialize_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def get_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def handle_data(self, **kwargs):  # pylint: disable=missing-function-docstring
        pass

    def derive_id(self, **kwargs):  # pylint: disable=missing-function-docstring
        pass

    def load_data(
        self, doc, key, element
    ):  # pylint: disable=missing-function-docstring
        pass

    def handle_document(self):  # pylint: disable=missing-function-docstring
        pass

    def build_document(
        self, queue_element
    ):  # pylint: disable=missing-function-docstring
        pass

    def build_datafile_doc(
        self, file_name, data_file_id, origin_type
    ):  # pylint: disable=missing-function-docstring
        pass

    def create_data_file_id(self, subset, file_type, origin_type, file_name):
        """
        This method creates a metar grib_to_cb datafile id from the parameters
        """
        try:
            base_name = os.path.basename(file_name)
            an_id = "DF:{s}:{f}:{o}:{b}".format(
                s=subset, f=file_type, o=origin_type, b=base_name
            )
            return an_id
        except Exception as _e:  # pylint: disable=broad-except
            logging.exception("%s create_data_file_id", self.__class__.__name__)
            return None
