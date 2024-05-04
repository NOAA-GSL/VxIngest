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
import math
from pathlib import Path
from pstats import Stats

import metpy.calc
import ncepbufr
import numpy.ma as ma
from metpy.units import units
from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
    initialize_data_array,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


#  ApiBuilder← RaobObsBuilder ← RaobsGslObsBuilder
class PrepbufrBuilder(Builder):  # pylint disable=too-many-instance-attributes
    """parent class for API builders"""

    def __init__(self, load_spec, ingest_document):
        # api builders do not init the ingest_document. That happens in build_document
        super().__init__(load_spec, ingest_document)

        self.load_spec = load_spec
        self.domain_stations = []
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
        """read data from the prepbufr file, filter messages for appropriate ones,
        and load them raw into a dictionary structure, so that they can be post processed
        for interpolations."""
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
        This routine processes the complete document (essentially a complete bufr file)
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            message_data_size = self.ncdf_data_set.dimensions["recNum"].size
            if message_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occurred
            new_document = initialize_data_array(new_document)
            for message in range(message_data_size):
                for key in self.template:
                    if key == "data":
                        new_document = self.handle_data(
                            doc=new_document, message=message
                        )
                        continue
                    new_document = self.handle_key(new_document, message, key)
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

    def handle_named_function(self, named_function_def, message):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        The _named_function_def looks like "&named_function:*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are separated by a ":" and
        the parameters are separated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... field_n:value_n}) and the return value from named_function
        will be substituted into the document.
        :record the record being processed.
        """
        # noinspection PyBroadException
        func = None
        try:
            func = named_function_def.split("|")[0].replace("&", "")
            params = named_function_def.split("|")[1].split(",")
            dict_params = {"recNum": message}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(_p, message)
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
            message = kwargs["message"]
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template:
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value, message)
                    else:
                        value = self.translate_template_item(value, message)
                except Exception as _e:  # pylint:disable=broad-except
                    value = None
                    logging.warning(
                        "%s Builder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key, message)
            else:
                data_key = self.translate_template_item(data_key, message)
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
        To process this raob_data object we need to iterate the data and process the station
        name along with all the other variables in the template.
        Args:
            queue_element - an ingest document id
        Returns:
            [dict]: document

        1) read the file to get all the obs data
        2) load the data into a dict by level and station
        3) post process the data to interpolate the levels
        4) handle_document for each station/level
        """
        # noinspection PyBroadException
        try:
            # read the api for all data for this valid fcst hour.
            self.bucket = self.load_spec["cb_connection"]["bucket"]
            self.scope = self.load_spec["cb_connection"]["scope"]
            # collection is set to "RAOB" in the run_ingest
            self.collection = self.load_spec["cb_connection"]["collection"]
            self.raw_obs_data = self.read_data_from_file(queue_element)
            if len(self.stations) == 0:
                stmnt = f"""SELECT {self.subset}.*
                    FROM `{self.bucket}`.{self.scope}.{self.collection}
                    WHERE type = 'MD'
                    AND docType = 'station'
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


# Concrete builders
class PrepbufrRaobsObsBuilderV01(PrepbufrBuilder):
    """
    This is the builder for RAOBS observation data that is ingested from prepbufr files
    """

    def __init__(self, load_spec, ingest_document):
        """
        This builder creates a set of V01 obs documents using the V01 raob station documents.
        This builder loads V01 station data into memory, and associates a station with an observation
        lat, lon, point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        If a station from a prepbufr file does not exist in the couchbase database
        a station document will be created from the prepbufr record data and
        the station document will be added to the document map. If a station location has changed
        the geo element will be updated to have an additional geo element that has the new location
        and time bracket for the location.
        :param ingest_document: the document from the ingest document
        :param load_spec: the load specification
        """
        PrepbufrBuilder.__init__(self, load_spec, ingest_document)
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        self.raw_data = {}
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def get_relative_humidity(self, rh, pressure, temperature, specific_humidity):
        """
        This method calculates the relative humidity from the specific humidity, if necessary
        :param rh: the relative humidity data - sometimes is not present
        :param pressure: the pressure data
        :param temperature: the temperature data
        :param specific_humidity: the specific humidity data
        :return: the relative humidity data

        example:
        relative_humidity_from_specific_humidity(pressure, temperature, specific_humidity)  all pint.Quantity
        relative_humidity_from_specific_humidity(1013.25 * units.hPa, 30 * units.degC, 18/1000).to('percent')
        """
        # try:
        #     p = pressure.filled(fill_value=math.nan) if ma.isarray(pressure) else ma.array([pressure])
        #     t = temperature.filled(fill_value=math.nan) if ma.isarray(temperature) else ma.array([temperature])
        #     sh = specific_humidity.filled(fill_value=math.nan) if ma.isarray(specific_humidity) else ma.array([specific_humidity])
        # except Exception as _e:  # pylint:disable=broad-except
        #     logging.error(
        #         f"PrepbufrRaobsObsBuilderV01.get_relative_humidity: Exception error: {_e}"
        #     )
        #     return ma.array([], fill_value=math.nan)
        # if ((ma.size(p) == 1 and math.isnan(p)) or
        #     (ma.size(t) == 1 and math.isnan(t)) or
        #     (ma.size(sh) == 1 and math.isnan(sh))
        # ):
        #     return ma.array([None], fill_value=math.nan)
        # rh = []
        # for i in range(len(p)):
        #     if (math.isnan(p[i]) or
        #         math.isnan(t[i]) or
        #         math.isnan(sh[i])
        #     ):
        #         _rh = None
        #     else:
        # if the pressure, temperature, or specific_humidity are totally masked
        # or the shape is () - it is a scalar - I don't know what to do
        if (
            (
                not ma.isMaskedArray(pressure)
                or ma.all(ma.is_masked(pressure))
                or pressure.shape == ()
            )
            or (
                not ma.isMaskedArray(temperature)
                or ma.all(ma.is_masked(temperature))
                or temperature.shape == ()
            )
            or (
                not ma.isMaskedArray(specific_humidity)
                or ma.all(ma.is_masked(specific_humidity))
                or specific_humidity.shape == ()
            )
        ):
            return ma.masked_array([], fill_value=math.nan)

        _rh = metpy.calc.relative_humidity_from_specific_humidity(
            pressure.data * units.hPa,
            temperature.data * units.degC,
            specific_humidity.data / 1000,
        ).to("percent")
        # rh.append(_rh)
        return _rh
        # return ma.masked_where(rh is math.nan, rh)

    def interpolate_heights(self, height, pressure, temperature, specific_humidity):
        """
        This method interpolates the heights that are missing in the height data
        using the hypsometric thickness equation
        :param height: the height data
        :return: the heights nd_array

        examples:

        mixing ratio from specific humidity:
            sh = [4.77, 12.14, 6.16, 15.29, 12.25] * units('g/kg')
            mixing_ratio_from_specific_humidity(sh).to('g/kg')
            <Quantity([ 4.79286195 12.28919078  6.19818079 15.52741416 12.40192356],
            'gram / kilogram')>

        thickness_hydrostatic with mixing ratio:
            # pressure
            p = [1008., 1000., 950., 900., 850., 800., 750., 700., 650., 600.,
                550., 500., 450., 400., 350., 300., 250., 200.,
                175., 150., 125., 100., 80., 70., 60., 50.,
                40., 30., 25., 20.] * units.hPa
            # temperature
            T = [29.3, 28.1, 23.5, 20.9, 18.4, 15.9, 13.1, 10.1, 6.7, 3.1,
                -0.5, -4.5, -9.0, -14.8, -21.5, -29.7, -40.0, -52.4,
                -59.2, -66.5, -74.1, -78.5, -76.0, -71.6, -66.7, -61.3,
                -56.3, -51.7, -50.7, -47.5] * units.degC
            # specify a layer
            layer = (p <= 1000 * units.hPa) & (p >= 500 * units.hPa)
            # compute the hydrostatic thickness
            mpcalc.thickness_hydrostatic(p[layer], T[layer])
            <Quantity(5755.94719, 'meter')>
        """
        # if the height is not a masked array - make it one
        if not ma.isMaskedArray(height):
            height = ma.masked_invalid(height)
        # save the original height mask in the raw data
        original_height_mask = height.mask
        # calculate the thickness for each layer and update the masked array
        # if the height is totally masked or the shape is () - it is a scalar - I don't know what to do
        if ma.all(ma.is_masked(height)) or height.shape == ():
            return height, original_height_mask
        # interpolate the heights
        # start at the bottom and work up
        # first - calculate the mixing ratio from the specific humidity for the entire array
        _mixing_ratio = metpy.calc.mixing_ratio_from_specific_humidity(
            specific_humidity
        ).to("g/kg")
        # now determine the missing layers

        i = 0
        while i < len(height):  # iterate the masked heights
            if math.isnan(height[i]):
                # get the height from the hydrostatic thickness using the layer below and the next layer above that has data
                # what is the next layer above that has data?
                j = i + 1
                while j < len(height) and ma.is_masked(height[j]):
                    j = j + 1
                # now height[i-1] (or height[0]) is the layer below that has data i.e. the bottom
                # and height[j] is the next layer above that has data i.e. the top
                top = j if j < len(height) else len(height) - 1
                bottom = 0 if i == 0 else i - 1
                p = pressure.data * units.hPa
                t = temperature.data * units.degC
                mr = _mixing_ratio.data * units.dimensionless
                layer = (pressure <= pressure[bottom]) & (pressure >= pressure[top])
                _height = metpy.calc.thickness_hydrostatic(
                    pressure=p[layer],
                    temperature=t[layer],
                    mixing_ratio=mr[layer],
                    molecular_weight_ratio=0.6219569100577033,
                )
                while (
                    i < j
                ):  # remember i is the bottom masked layer and j is the next layer above that has data
                    height[i] = (
                        # does this need to be added to the height of the layer below?
                        _height.magnitude + height[i - 1]
                        if i > 0
                        else _height.magnitude
                    )  # assigning a valid value to height[i] unmasks that value
                    # go to the next one
                    i = i + 1
            else:
                i = i + 1  # this one wsa not masked so go to the next one
        return height, original_height_mask

    def read_data_from_bufr(self, bufr, template):
        """
        This method reads the data from the bufr file according to a provided template.
        A template is a dict keyed by the desired field name with a value that is a
        dict with a mnemonic and an intent. The mnemonic is the bufr mnemonic for the field
        and the intent is the datatype of the field in the resulting data document.
        For example station_id "SID" returns a float but the intent is str.
        :param bufr: the bufr file
        :template: a dictionary of header keys with their corresponding mnemonics and intended types
        refer to https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/bufrtab_tableb.html
        example:
        hdr_template={
            "station_id": {"mnemonic":"SID","intent":"str"},
            "lon": {"mnemonic":"XOB", "intent":"float"},
            "lat": {"mnemonic":"YOB", "intent":"float"},
            "obs-cycle_time": {"mnemonic":"DHR", "intent":"float"},
            "station_type": {"mnemonic":"TYP", "intent":"int"},
            "elevation": {"mnemonic":"ELV", "intent":"float"},
            "report_type": {"mnemonic":"T29", "intent":"int"}
        }
        q_marker_template = {
            "pressure_q_marker": {"mnemonic":"PQM","type":int},
            "specific_humidity_q_marker": {"mnemonic":"QQM","type":int},
            "temperature_q_marker": {"mnemonic":"TQM","type":int},
            "height_q_marker": {"mnemonic":"ZQM","type": int},
            "u_v_wind_q_marker": {"mnemonic":"WQM", "type":int},
        }
        obs_err_template = {
            "pressure_obs_err": {"mnemonic":"POE","type": float},
            "relative_humidity_obs_err": {"mnemonic":"QOE","type": float},
            "temperature_obs_err": {"mnemonic":"TOE","type": float},
            "winds_obs_err": {"mnemonic":"WOE","type": float},
        }
        obs_data_template = {
                "temperature": "TOB",
                "dewpoint": "TDO",
                "rh": "RHO",
                "specific_humidity": "QOB",
                "pressure": "POB",
                "height": "ZOB",
                "wind_speed": "FFO",
                "U-Wind": "UOB",
                "V-Wind": "VOB",
                "wind_direction": "DDO",
            }

        :return: the data
        """
        # see read_subset https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L449
        mnemonics = [o["mnemonic"] for o in template.values()]
        bufr_data = bufr.read_subset(" ".join(mnemonics)).squeeze()
        data = {}
        for i, mnemonic in enumerate(mnemonics):
            field = [k for k, v in template.items() if v["mnemonic"] == mnemonic]
            if field[0] == "rh":
                rh_index = mnemonics.index("RHO")
                pressure_index = mnemonics.index("POB")
                temperature_index = mnemonics.index("TOB")
                specific_humidity_index = mnemonics.index("QOB")
                data[field[0]] = self.get_relative_humidity(
                    bufr_data[rh_index],  # rh - sometimes is missing
                    bufr_data[pressure_index],  # pressure
                    bufr_data[temperature_index],  # temperature
                    bufr_data[specific_humidity_index],  # specific_humidity
                )
            else:
                if field[0] == "height":
                    height_index = mnemonics.index("ZOB")
                    pressure_index = mnemonics.index("POB")
                    temperature_index = mnemonics.index("TOB")
                    specific_humidity_index = mnemonics.index("QOB")
                    data[field[0]], _original_mask = self.interpolate_heights(
                        bufr_data[
                            height_index
                        ],  # height - fields are sometimes missing
                        bufr_data[pressure_index],  # pressure
                        bufr_data[temperature_index],  # temperature
                        bufr_data[specific_humidity_index],  # specific_humidity
                    )
                else:
                    match template[field[0]]["intent"]:
                        case "int":
                            data[field[0]] = bufr_data[i]
                        case "float":
                            data[field[0]] = bufr_data[i]
                        case "str":
                            data[field[0]] = str(bufr_data[i], encoding="utf-8").strip()
                        case _:
                            data[field][0] = bufr_data[i]
        return data

    def read_data_from_file(self, queue_element, templates):
        """read data from the prepbufr file, filter messages for appropriate ones,
        and load them raw into a raw dictionary structure. Use hypsometric equation to
        calculate heights from pressure for determining unknown heights. Load everything into
        a dictionary structure, so that mandatory levels can be interpolated for every 10mb
         using weighted logarithmic interpolation.
        Args:
            queue_element: the file name to read
        Transformations: 1) pressure to height 2) temperature to dewpoint 3) meters per second to miles per hour
        creates a self raw document_map
        """
        bufr = ncepbufr.open(queue_element)
        raw_data = {}
        while bufr.advance() == 0:  # loop over messages.
            if bufr.msg_type != "ADPUPA":
                continue
            # see load subset https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L389
            while bufr.load_subset() == 0:  # loop over subsets in message.
                # read the header data
                header_data = self.read_data_from_bufr(bufr, templates["header"])
                raw_data[header_data["station_id"]] = {}
                raw_data[header_data["station_id"]]["header"] = header_data
                # read the q_marker data
                q_marker_data = self.read_data_from_bufr(bufr, templates["q_marker"])
                raw_data[header_data["station_id"]]["q_marker"] = q_marker_data
                # read the obs_err data
                obs_err_data = self.read_data_from_bufr(bufr, templates["obs_err"])
                raw_data[header_data["station_id"]]["obs_err"] = obs_err_data
                # read the obs data
                obs_data = self.read_data_from_bufr(bufr, templates["obs_data"])
                raw_data[header_data["station_id"]]["obs_data"] = obs_data
        bufr.close()
        return raw_data

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        """
        This method will build a dataFile document for prepbufr builder. The dataFile
        document will represent the file that is ingested by the prepbufr builder. The document
        is intended to be added to the output folder and imported with the other documents.
        The VxIngest will examine the existing dataFile documents to determine if a specific file
        has already been ingested.
        """
        mtime = Path(file_name).stat().st_mtime
        df_doc = {
            "id": data_file_id,
            "mtime": mtime,
            "subset": self.subset,
            "type": "DF",
            "fileType": "netcdf",
            "originType": origin_type,
            "loadJobId": self.load_spec["load_job_doc"]["id"],
            "dataSourceId": "madis3",
            "url": file_name,
            "projection": "lambert_conformal_conic",
            "interpolation": "nearest 4 weighted average",
        }
        return df_doc

    def initialize_document_map(self):
        """
        reset the document_map for a new file
        """
        self.document_map = {}

    def get_document_map(self):
        """
        In case there are leftovers we have to process them first.
        :return: the document_map
        """
        try:
            if len(self.same_time_rows) != 0:
                self.handle_document()
            return self.document_map
        except Exception as _e:
            logger.exception(
                "%s get_document_map: Exception in get_document_map: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def load_data(self, doc, key, element):
        """
        This method adds an observation to the data dict -
        in fact we use a dict to hold data elems to ensure
        the data elements are unique per station name.
        Using a map ensures that the last
        entry in the netcdf file is the one that gets captured.
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if "data" not in doc or doc["data"] is None:
            doc["data"] = {}
        if element["name"] not in doc["data"]:
            # we only want the closest record (to match the legacy-sql data)
            doc["data"][element["name"]] = element
        else:
            # is this one closer to the target time?
            top_of_hour = doc["fcstValidEpoch"]
            if abs(top_of_hour - element["Reported Time"]) < abs(
                top_of_hour - doc["data"][element["name"]]["Reported Time"]
            ):
                doc["data"][element["name"]] = element
        return doc

    # named functions
    def meterspersecond_to_milesperhour(self, params_dict):
        """Converts meters per second to mile per hour performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        # Meters/second to miles/hour
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = value * 2.237
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

        return None

    def kelvin_to_fahrenheit(self, params_dict):
        """Converts kelvin to farenheight performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = (float(value) - 273.15) * 1.8 + 32
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function kelvin_to_farenheight:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def handle_station(self, params_dict):
        """
        This method uses the station name in the params_dict
        to find a station with that name from self.stations (which are all the station documents
        from couchbase).
        If the station does not exist it will be created with data from the
        prepbufr file. If the station exists the lat, lon, and elev from the prepbufr file
        will be compared to that in the existing station and if an update of the geo list is required it will be updated.
        Any modified or newly created stations get added to the document_map and automatically upsert'ed.
        :param params_dict: {'bufr_subset': 'bufr_subset_loaded`}
        :return:
        """
        station_id = params_dict["stationId"]
        an_id = None
        fcst_valid_epoch = self.raw_data[station_id]["time"]
        try:
            elev = self.raw_data[station_id]["elevation"]
            lat = self.raw_data[station_id]["lat"]
            lon = self.raw_data[station_id]["lon"]
            station = None
            station_index = None
            for idx, a_station in enumerate(self.stations):
                if a_station["station_id"] == station_id:
                    station = a_station
                    station_index = idx
                    break

            if station is None:
                # get the raw fields for comparing or adding new
                an_id = f"MD:V01:{self.subset}:station:{station_id}"
                new_station = {
                    "id": an_id,
                    "wmoid": station_id,
                    "name": self.raw_data["header"][station_id]["name"],
                    "description": self.raw_data["header"][station_id]["description"],
                    "near_airport": self.raw_data["header"][station_id]["near_airport"],
                    "gps_date": self.raw_data["header"][station_id]["gps_date"],
                    "updateTime": self.raw_data["header"][station_id]["updateTime"],
                    "docType": "station",
                    "subset": self.subset,
                    "type": "MD",
                    "version": "V01",
                    "geo": [
                        {
                            "firstTime": fcst_valid_epoch,
                            "elev": elev,
                            "lat": lat,
                            "lon": lon,
                            "lastTime": fcst_valid_epoch,
                        }
                    ],
                }
                # add the new station to the document map with the new id
                if an_id not in self.document_map:
                    self.document_map[an_id] = new_station
                self.stations.append(new_station)
            else:
                # station does exist but is there a matching geo?
                # if there is not a matching geo create a new geo
                # if there is a matching geo then update the matching geo time range
                matching_location = False
                requires_new_geo = False
                for geo_index in range(len(self.stations[station_index]["geo"])):
                    geo = self.stations[station_index]["geo"][geo_index]
                    if geo["lat"] == lat and geo["lon"] == lon and geo["elev"] == elev:
                        matching_location = True
                        break
                if matching_location:
                    if (
                        fcst_valid_epoch
                        <= self.stations[station_index]["geo"][geo_index]["firstTime"]
                    ):
                        self.stations[station_index]["geo"][geo_index]["firstTime"] = (
                            fcst_valid_epoch
                        )
                    else:
                        self.stations[station_index]["geo"][geo_index]["lastTime"] = (
                            fcst_valid_epoch
                        )
                else:
                    # This station requires a new geo because there are no matching locations i.e. the location has changed
                    requires_new_geo = True
                if requires_new_geo:
                    self.stations[station_index]["geo"].append(
                        {
                            "firstTime": fcst_valid_epoch,
                            "elev": elev,
                            "lat": lat,
                            "lon": lon,
                            "lastTime": fcst_valid_epoch,
                        }
                    )
                # add the modified station to the document map with its existing id
                self.stations[station_index]["updateTime"] = fcst_valid_epoch
                an_id = self.stations[station_index]["id"]
                self.document_map[an_id] = self.stations[station_index]
            return params_dict["stationName"]
        except Exception as _e:
            logger.exception(
                "%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name: params: %s",
                self.__class__.__name__,
                str(params_dict),
            )
            return ""
