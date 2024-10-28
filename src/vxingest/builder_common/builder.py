"""
Builder - Parent class for all Builders
"""

import logging
import math
from pathlib import Path

import numpy.ma as ma


class Builder:
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

    def initialize_document_map(self):
        pass

    def get_document_map(self):
        pass

    def handle_data(self, **kwargs):
        pass

    def derive_id(self, **kwargs):
        pass

    def load_data(self, doc, key, element):
        pass

    def handle_document(self):
        pass

    def build_document(self, queue_element):
        pass

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        pass

    def create_data_file_id(self, subset, file_type, origin_type, file_name):
        """
        This method creates a datafile id from the parameters
        """
        try:
            base_name = Path(file_name).name
            an_id = f"DF:{subset}:{file_type}:{origin_type}:{base_name}"
            return an_id
        except Exception as _e:
            logging.exception("%s create_data_file_id", self.__class__.__name__)
            return None

    def is_a_number(self, v):
        return (
            v is not None
            and isinstance(v, (int, float))
            and not math.isnan(v)
            and v is not ma.masked
        )
