"""
Program Name: Class ApiBuilder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import abc
import copy
import cProfile
import logging
from pathlib import Path
from pstats import Stats

from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
    initialize_data_array,
)


#  ApiBuilder← RaobObsBuilder ← RaobsGslObsBuilder
class PrepbufrBuilder(Builder):  # pylint disable=too-many-instance-attributes
    """parent class for API builders"""

    def __init__(self, load_spec, ingest_document):
        # api builders do not init the ingest_document. That happens in build_document
        super().__init__(load_spec, ingest_document)

        self.load_spec = load_spec
        self.domain_stations = []
        # CTC builder specific
        # These following need to be declared here but assigned in
        # build_document because each ingest_id might redifne them
        self.ingest_document = None
        self.template = None
        self.subset = None
        self.model = None
        self.sub_doc_type = None
        self.model_fcst_valid_epochs = []
        self.stations = {}
        self.obs_data = {}
        # used to stash each fcstValidEpoch obs_data for the handlers
        self.obs_station_names = []  # used to stash sorted obs names for the handlers
        self.thresholds = None
        self.not_found_stations = set()
        self.not_found_station_count = 0
        self.bucket = None
        self.scope = None
        self.collection = None

    @abc.abstractmethod
    def read_data_from_file(self, queue_element):
        """read data from the prpebufr file, filter messages for appropriate ones,
        and load them raw into a dictionary structure, so that they can be post processed
        for interpolations and other data manipulations."""
        return

    @abc.postprocess_raw_data
    def postprocess_raw_data(self):
        """This routine is called after the raw data is read from the file. It is used to
        filter the data, and to interpolate the data to the levels specified in the template."""
        return

    def derive_id(self, **kwargs):
        """
        This is a private method to derive a document id from the current valid_fcst_time and level.
        A *field represents a direct substitution and a &function|params...
        represents a handler function.
        Args:
            template_id (string): this is an id template string
        Returns:
            [string]: The processed id with substitutions made for elements in the id template
        """
        try:
            template_id = kwargs["template_id"]
            fcst_time = kwargs["valid_fcst_time"]
            parts = template_id.split(":")
            new_parts = []
            for part in parts:
                if part.startswith("&"):
                    value = str(self.handle_named_function(part, fcst_time))
                else:
                    if part.startswith("*"):
                        value = str(self.translate_template_item(part, fcst_time))
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception("ApiBuilder.derive_id: Exception  error: %s")
            return None

    def translate_template_item(self, variable, api_record):
        """
        This method translates template replacements (*item).
        It can translate keys or values.
        :param variable: a value from the template - should be a record field
        :param api_record
        :return:
        """
        replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(variable, str):
                replacements = variable.split("*")[1:]
            # this is a literal, doesn't need to be returned
            if len(replacements) == 0:
                return variable
            # pre assign these in case it isn't a replacement - makes it easier
            value = variable
            if len(replacements) > 0:
                for _ri in replacements:
                    value = self.model_data[_ri]
                    # convert each station value to iso if necessary
                    if _ri.startswith("{ISO}"):
                        value = variable.replace("*" + _ri, convert_to_iso(value))
                    else:
                        value = variable.replace("*" + _ri, str(value))
            return value
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "CtcBuilder.translate_template_item: Exception  error: %s", str(_e)
            )
            return None

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete api data document)
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            rec_num_data_size = self.ncdf_data_set.dimensions["recNum"].size
            if rec_num_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for rec_num in range(rec_num_data_size):
                for key in self.template:
                    if key == "data":
                        new_document = self.handle_data(
                            doc=new_document, rec_num=rec_num
                        )
                        continue
                    new_document = self.handle_key(new_document, rec_num, key)
            # put document into document map
            if new_document["id"]:
                logging.info(
                    "NetcdfBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logging.info(
                    "NetcdfBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "NetcdfBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_key(self, doc, fcst_valid_time, level, key):
        """
        This routine handles keys by substituting
        the data that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param station: The current station
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """
        # noinspection PyBroadException
        try:
            if key == "id":
                an_id = self.derive_id(
                    template_id=self.template["id"],
                    fcst_valid_time=fcst_valid_time,
                    level=level,
                )
                if an_id not in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_key(
                        tmp_doc, fcst_valid_time, level, sub_key
                    )  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key], fcst_valid_time, level)
            else:
                doc[key] = self.translate_template_item(
                    doc[key], fcst_valid_time, level
                )
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s ApiBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
            )
        return doc

    def handle_named_function(self, named_function_def, rec_num):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        The _named_function_def looks like "&named_function:*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are seperated by a ":" and
        the parameters are seperated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :record the record being processed.
        """
        # noinspection PyBroadException
        func = None
        try:
            func = named_function_def.split("|")[0].replace("&", "")
            params = named_function_def.split("|")[1].split(",")
            dict_params = {"recNum": rec_num}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(_p, rec_num)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s handle_named_function: %s params %s: Exception instantiating builder:",
                self.__class__.__name__,
                func,
                params,
            )
        return replace_with

    def handle_data(self, **kwargs):
        """This method iterates the template entries, deciding for each entry to either
        handle_named_function (if the entry starts with a '&') or to translate_template_item
        if it starts with an '*'. It handles both keys and values for each template entry.
        Args:
            doc (Object): this is the data document that is being built
        Returns:
            (Object): this is the data document that is being built
        """
        try:
            doc = kwargs["doc"]
            rec_num = kwargs["rec_num"]
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template:
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value, rec_num)
                    else:
                        value = self.translate_template_item(value, rec_num)
                except Exception as _e:  # pylint:disable=broad-except
                    value = None
                    logging.warning(
                        "%s Builder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key, rec_num)
            else:
                data_key = self.translate_template_item(data_key, rec_num)
            if data_key is None:
                logging.warning(
                    "%s Builder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_document(self, queue_element):
        """This is the entry point for the Builders from the ingestManager.
        These documents are id'd by fcstValidEpoch and level. The data section is a dictionary
        keyed by station name each element of which contains variable data and the station name.
        To process this raob_data object we need to itterate the data and process the station
        name along with all the other variables in the template.
        Args:
            queue_element - an ingest document id
        Returns:
            [dict]: document

        1) read the file to get all the obs data
        2) load the data into a dict by level and station
        3) post process the data to interolate the levels
        4) handle_document for each station/level
        """
        # noinspection PyBroadException
        try:
            # read the api for all data for this valid fcst hour.
            self.bucket = self.load_spec["cb_connection"]["bucket"]
            self.scope = self.load_spec["cb_connection"]["scope"]
            self.collection = self.load_spec["cb_connection"]["collection"]
            self.raw_obs_data = self.read_data_from_file(queue_element)
            self.postprocess_raw_data()
            if len(self.stations) == 0:
                stmnt = f"""SELECT {self.subset}.*
                    FROM `{self.bucket}`.{self.scope}.{self.collection}
                    WHERE type = 'MD'
                    AND docType = 'raobStation'
                    AND subset = '{self.subset}'
                    AND version = 'V01';"""
                result = self.load_spec["cluster"].query(stmnt)
                self.stations = list(result)

            self.initialize_document_map()
            logging.info(
                "%s building documents for file %s",
                self.__class__.__name__,
                queue_element,
            )
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    self.handle_document()
                    with Path.open("profiling_stats.txt", "w") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_document()
            # pylint: disable=assignment-from-no-return
            document_map = self.get_document_map()
            data_file_id = self.create_data_file_id(
                self.subset, "netcdf", "madis", queue_element
            )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element, data_file_id=data_file_id, origin_type="madis"
            )
            document_map[data_file_doc["id"]] = data_file_doc
            return document_map
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: Exception with builder build_document: file_name: %s",
                self.__class__.__name__,
                queue_element,
            )
            return {}
