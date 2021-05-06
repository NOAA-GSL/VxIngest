"""
Program Name: Class sql_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

import copy
import datetime as dt
import logging
import sys
import re
import pymysql
import math
import time
from decimal import Decimal
# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions
from pymysql.constants import CLIENT

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SQL_PORT = 3306


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str


def derive_id(an_id, row, interpolated_time):
    # Private method to derive a document id from the current row,
    # substituting *values from the corresponding row field as necessary.
    # noinspection PyBroadException
    try:
        _parts = an_id.split(':')
        new_parts = []
        for _part in _parts:
            if _part.startswith("*"):
                if _part == "*time":
                    value = str(interpolated_time)
                else:
                    value = GsdBuilder.translate_template_item(_part, row, interpolated_time)
                new_parts.append(str(value))
            else:
                new_parts.append(str(_part))
        _new_id = ":".join(new_parts)
        return _new_id
    except:
        e = sys.exc_info()
        logging.error("GsdBuilder.derive_id: Exception  error: " + str(e))


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
    return doc


class GsdBuilder:
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.statement_replacement_params = statement_replacement_params
        
        try:
            mysql_credentials = self.load_spec['mysql_connection']
            host = mysql_credentials['host']
            if 'port' in mysql_credentials.keys():
                port = int(mysql_credentials['port'])
            else:
                port = SQL_PORT
            user = mysql_credentials['user']
            passwd = mysql_credentials['password']
            local_infile = True
            self.connection = pymysql.connect(host=host, port=port, user=user, passwd=passwd, local_infile=local_infile,
                                              autocommit=True, charset='utf8mb4',
                                              cursorclass=pymysql.cursors.SSDictCursor,
                                              client_flag=CLIENT.MULTI_STATEMENTS)
            self.cursor = self.connection.cursor(pymysql.cursors.SSDictCursor)
        except pymysql.OperationalError as pop_err:
            logging.error(self.__class__.__name__ + "*** %s in connect_mysql ***" + str(pop_err))
            sys.exit("*** Error when connecting to mysql database: ")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def load_data(self, doc, key, element):
        pass
    
    def get_name(self, params_dict):
        pass
    
    def get_document_map(self):
        pass
    
    @staticmethod
    def translate_template_item(value, row, interpolated_time):
        """
        This method translates template replacements (*item).
        It can translate keys or values. In addition it can eval a named function
        with locals(). Example get_name(param_dict)
        :param interpolated_time: either the time in the row['time'] or the interpolated_time if required.
        :param row: a row from the result set
        :param value: a value from the template
        :return:
        """
        _replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(value, str):
                _replacements = value.split('*')[1:]
            # skip the first replacement, its never
            # really a replacement. It is either '' or not a
            # replacement
            _make_str = False
            if len(_replacements) > 0:
                for _ri in _replacements:
                    if _ri.startswith('{ISO}') or type(row[_ri]) is str:
                        _make_str = True
                        break
                for _ri in _replacements:
                    if _ri.startswith("{ISO}"):
                        if _ri == '{ISO}time':
                            value = value.replace("*" + _ri, convert_to_iso(interpolated_time))
                        else:
                            value = value.replace("*" + _ri, convert_to_iso(row[_ri]))
                    else:
                        if _ri == 'time':
                            value = interpolated_time
                        else:
                            if _make_str:
                                value = value.replace('*' + _ri, str(row[_ri]))
                            else:
                                if isinstance(row[_ri], Decimal):
                                    value = float(row[_ri])
                                else:
                                    value = row[_ri]
            return value
        except Exception as e:
            logging.error("GsdBuilder.translate_template_item: Exception  error: " + str(e))
        return value
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        This method is always overridden
        :param row: A result set row
        :return:
        """
        pass
    
    def handle_document(self, interpolated_time, rows):
        """
        :param interpolated_time: The time field in a row, if there is one,
        may have been interpolated in the ingest_manager to the closest time
        to the cadence within the delta. If interpolation was not required then the
        interpolated_time will be the same as the row['time']
        :param rows: This is a row array that contains rows from the result set
        that all have the same a_time. There may be many stations in this row
        array, AND importantly the document id derived from this a_time may
        already exist in the document. If the id does not exist it will be
        created, if it does exist, the data will be appended.
        builder's documents will be added, the GsdIngestManager will do the
        upsert
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            doc = copy.deepcopy(self.template)
            if len(rows) == 0:
                return
            doc = initialize_data(doc)
            for r in rows:
                for k in self.template.keys():
                    if k == "data":
                        doc = self.handle_data(doc, r, interpolated_time)
                        continue
                    doc = self.handle_key(doc, r, k, interpolated_time)
            # remove id (it isn't needed inside the doc, we needed it in the template
            # to tell us how to format the id)
            del doc['id']
            # put document into document map
            self.document_map[self.id] = doc
        except Exception as e:
            logging.error(self.__class__.__name__ + "GsdBuilder.handle_document: Exception instantiating "
                                                    "builder: " + self.__class__.__name__ + " error: " + str(e))
            raise e
        
    def handle_key(self, doc, row, key, interpolated_time):
        """
        This routine handles keys by substituting row fields into the values
        in the template that begin with *
        :param doc: the current document
        :param row: The data row from the mysql result set
        :param interpolated_time: The closest time to the cadence within the
        delta.
        :param key: A key to be processed, This can be a key to a primitive
        or to another dictionary
        """
        # noinspection PyBroadException
        try:
            if key == 'id':
                self.id = derive_id(self.template['id'], row, interpolated_time)
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                _tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in _tmp_doc.keys():
                    _tmp_doc = self.handle_key(_tmp_doc, row, sub_key, interpolated_time)  # recursion
                doc[key] = _tmp_doc
            if not isinstance(doc[key], dict) and isinstance(doc[key], str) and doc[key].startswith('&'):
                doc[key] = self.handle_named_function(doc[key], interpolated_time, row)
            else:
                doc[key] = self.translate_template_item(doc[key], row, interpolated_time)
            return doc
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GsdBuilder.handle_key: Exception in builder:  error: " + str(e))
        return doc
    
    def handle_named_function(self, _data_key, interpolated_time, row):
        # noinspection PyBroadException
        try:
            _func = _data_key.split(':')[0].replace('&', '')
            _params = _data_key.split(':')[1].split(',')
            _dict_params = {}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(_p, row, interpolated_time)
            _data_key = getattr(self, _func)(_dict_params)
            if _data_key is None:
                logging.warning("self.__class__.__name__ + GsdBuilder: Using " + _func + " - None returned for " + str(
                    _dict_params))
                _data_key = row['name'] + "0"
                return _data_key
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "handle_named_function: Exception instantiating builder:  error: " + str(e))
        return _data_key
    
    def handle_data(self, doc, row, interpolated_time):
        # noinspection PyBroadException
        try:
            _data_elem = {}
            _data_key = next(iter(self.template['data']))
            _data_template = self.template['data'][_data_key]
            for key in _data_template.keys():
                value = _data_template[key]
                if value.startswith('&'):
                    value = self.handle_named_function(value, interpolated_time, row)
                else:
                    value = self.translate_template_item(value, row, interpolated_time)
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, interpolated_time, row)
            else:
                _data_key = self.translate_template_item(_data_key, row, interpolated_time)
            if _data_key is None:
                logging.warning(self.__class__.__name__ + "GsdBuilder.handle_data - _data_key is None")
            doc = self.load_data(doc, _data_key, _data_elem)
            return doc
        
        except Exception as e:
            logging.error(self.__class__.__name__ + "handle_data: Exception instantiating builder:  error: " + str(e))
        return doc
    
    def build_document(self, _ingest_document):
        _statement = ""
        # noinspection PyBroadException
        try:
            # process the document
            _statement = _ingest_document['statement']
            # replace any statement params - replacement params are like {param}=replacement
            for _k in self.statement_replacement_params.keys():
                _statement = _statement.replace(_k, str(self.statement_replacement_params[_k]))
            _statements = _statement.split(';')
            for s in _statements:
                if s.strip().upper().startswith('SET'):
                    _value = re.split("=", s)[1].strip()
                    _m = re.findall(r'[@]\w+', s)[0]
                    _statement = _statement.replace(s + ';', '')
                    _statement = _statement.replace(_m, _value)
            _query_start_time = int(time.time())
            logging.info(self.__class__.__name__ + "executing query: start time: " + str(_query_start_time))
            self.cursor.execute(_statement)
            _query_stop_time = int(time.time())
            logging.info(self.__class__.__name__ + "executing query: stop time: " + str(_query_stop_time))
            logging.info(self.__class__.__name__ + "executing query: elapsed seconds: " + str(
                _query_stop_time - _query_start_time))
        except Exception as e:
            logging.error(
                self.__class__.__name__ + ": Exception processing the statement: error: " + str(e))
        # noinspection PyBroadException
        try:
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                self.handle_row(row)
            # The document_map could potentially have a lot of documents in it
            # depending on how the builder collated the rows into documents
            # i.e. by time like for obs, or by time and fcst_len like for models,
            # or all in one like for stations
            _document_map = self.get_document_map()
            return _document_map
            self.close()
        except Exception as e:
            logging.error(self.__class__.__name__ + ": Exception with builder handle_row: error: " + str(e))
            self.close()
            return {}


# Concrete builders
class GsdObsBuilderV01(GsdBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a station with an observation
        lat, lon point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.cluster = cluster
        self.stations = []
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['delta']
        self.cadence = ingest_document['cadence']
        # noinspection PyBroadException
        try:
            # Retrieve the required station data
            n1ql_query = ingest_document['station_query']
            row_iter = cluster.query(n1ql_query, QueryOptions(read_only=True))
            for _station in row_iter:
                self.stations.append(_station)
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GsdStationsBuilderV01: error getting stations, " + str(e))
            raise e
        
    def interpolate_time(self, a_time):
        _remainder_time = a_time % self.cadence
        _cadence_time = a_time / self.cadence * self.cadence
        if _remainder_time < self.delta:
            _t = a_time - _remainder_time
        else:
            _t = a_time - _remainder_time + self.cadence
        return _t
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        Rows are collated according to interpolated time. Each set of rows that have the same interpolated time
        are passed to the handle_document to be processed into a single document.
        :param row: A result set row
        :return:
        """
        self.interpolated_time = self.interpolate_time(int(row['time']))
        if self.time == 0:
            self.time = self.interpolated_time
        if self.interpolated_time != self.time:
            # we have a new interpolated time so build a document
            self.handle_document(self.interpolated_time, self.same_time_rows)
            self.time = 0
            self.same_time_rows = []
        self.same_time_rows.append(row)
    
    def get_document_map(self):
        """
        In case there are leftovers we have to process them first.
        :return: the document_map
        """
        if len(self.same_time_rows) != 0:
            self.handle_document(self.interpolated_time, self.same_time_rows)
        return self.document_map
    
    def get_name(self, params_dict):
        """
         This method uses the lat and lon that are in the params_dict
         to find a station at that geopoint using math.isclose().
         :param params_dict:
         :return:
         """
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # noinspection PyBroadException
        try:
            for elem in self.stations:
                if not isinstance(elem, dict):
                    continue
                if math.isclose(elem['lat'], _lat, abs_tol=0.05) and math.isclose(elem['lon'], _lon, abs_tol=0.05):
                    return elem['name']
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GsdObsBuilderV02.get_name: Exception finding station to match lat and lon  "
                                          "error: " + str(e) + " params: " + str(params_dict))
        # if we got here then there is an error - should have returned above
        logging.error(
            self.__class__.__name__ + "GsdObsBuilderV02.get_name: No station found to match lat and lon for " + str(
                params_dict))
        return None
    
    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdModelBuilderV01(GsdBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates a set of V03 model documents using the V01 station documents
        This builder is very much like the GsdObsBuilderV04 except that it builds model documents.
        In each document the specific model data is an array of objects each of which is the model data
        for a specific station. The particular model name is supplied in the template.
        :param ingest_document: the document from the ingest document like MD:V04:METAR:HRRR:ingest
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.cluster = cluster
        self.stations = []
        self.same_time_rows = []
        self.time = 0
        self.fcst_len = 0
        # noinspection PyBroadException
        try:
            # Retrieve the required station data
            n1ql_query = 'SELECT raw {mdata.name, mdata.geo.lat, mdata.geo.lon} FROM mdata ' \
                         'WHERE type="DD" AND docType="station" AND subset="METAR" AND version ="V03"'
            row_iter = cluster.query(n1ql_query, QueryOptions(read_only=True))
            for _station in row_iter:
                self.stations.append(_station)
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GsdStationsBuilderV01: error getting stations, " + str(e))
    
    def get_document_map(self):
        """
        In case there are leftovers we have to process them first.
        :return: the document_map
        """
        if len(self.same_time_rows) != 0:
            self.handle_document(self.time, self.same_time_rows)
        return self.document_map
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        Rows are collated according to interpolated time and fcst_len. Each set of rows that have the same interpolated
        time and fcst_len are passed to the handle_document to be processed into a single document.
        :param row: A result set row
        :return:
        """
        if self.time == 0:
            self.time = int(row['time'])
            self.fcst_len = int(row['fcst_len'])
        if int(row['time']) != self.time or int(row['fcst_len']) != self.fcst_len:
            self.handle_document(self.time, self.same_time_rows)
            self.time = 0
            self.same_time_rows = []
        self.same_time_rows.append(row)
    
    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = {}
        doc['data'][key] = element
        return doc


class GsdStationsBuilderV01(GsdBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates multiple documents one per station with the id being
        MD:V03:METAR:stations:*name where the variable part of the id
        is the the station name which, is an ICAO, like CWDP. So an ID
        might be "MD:V03:METAR:stations:CWDP". The lat, lon, and elevation are
        encoded into a "geo" element such that we can create a geojson search index over the data.
        :type ingest_document: object - This is the ingest document from "MD:V01:METAR:stations:ingest"
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.same_time_rows = []
    
    def get_document_map(self):
        """
        singleDocument builders (each row becomes a document) never have leftovers
        :return:  the document_map
        """
        return self.document_map
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        With the stationBuilder each row becomes a document so each row  passed to the handle_document to be processed
        into a single document.
        :param row: A result set row
        :return:
        """
        GsdBuilder.handle_document(self, 0, [row])
