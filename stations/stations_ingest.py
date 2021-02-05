import argparse
import logging
import sys
import time
import yaml
import urllib.request
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


def parse_args(args):
    begin_time = str(datetime.now())
    logging.basicConfig(level=logging.INFO)
    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin a_time: %s" + begin_time)
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument("spec_file",
                        help="Please provide required load_spec filename "
                             "- something.xml or something.yaml")
    parser.add_argument("-c", "--credentials_file", type=str,
                        help="Please provide required credentials_file")
    parser.add_argument("-p", "--cert_path", type=str, default='',
                        help="path to server public cert")
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class VXStationsIngest(object):
    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.spec_file_name = ""
        self.spec_file_data = {}
        self.credentials_file = ""
        self.credential_file_data = {}
        self.cert_path = None
        self.station_document = {}
        self.collection = None
        
        """
            !   CD = 2 letter state (province) abbreviation
            !   STATION = 16 character station long name
            !   ICAO = 4-character international id
            !   IATA = 3-character (FAA) id
            !   SYNOP = 5-digit international synoptic number
            !   LAT = Latitude (degrees minutes)
            !   LON = Longitude (degree minutes)
            !   ELEV = Station elevation (meters)
            !   M = METAR reporting station.   Also Z=obsolete? site
            !   N = NEXRAD (WSR-88D) Radar site
            !   V = Aviation-specific flag (V=AIRMET/SIGMET end point,
            A=ARTCC T=TAF U=T+V)
            !   U = Upper air (rawinsonde=X) or Wind Profiler (W) site
            !   A = Auto (A=ASOS, W=AWOS, M=Meso, H=Human, G=Augmented) (H/G
            not yet impl.)
            !   C = Office type F=WFO/R=RFC/C=NCEP Center
            !   Digit that follows is a priority for plotting (0=highest)
            !   Country code (2-char) is last column
        """
    
    def load_stations(self):
        line_slices = self.spec_file_data['load_spec']['line_slices']
        _url = self.spec_file_data['load_spec']['station_url']
        for _line in urllib.request.urlopen(_url):
            _station_entry = {}
            _line_str = _line.decode('utf-8').strip()
            # skip lines that are comments, blank, or start with CD
            if not _line_str.strip() or str.startswith(_line_str, 'CD') or \
                    str.startswith(_line_str, '!'):
                continue
            if len(_line_str) < 30:
                # this is a date line
                _parts = _line_str.split()
                _station_entry['province'] = _parts[0]
                _station_entry['date'] = _parts[1]
            else:
                # this is a station line
                for k in line_slices.keys():
                    _start = line_slices[k]['start']
                    _end = line_slices[k]['len'] + line_slices[k]['start']
                    value = _line_str[_start:_end]
                    _station_entry[k] = value
                self.station_document[_station_entry['ICAO']] = _station_entry
                
    def read_credentials_file(self):
        #
        #  Read the credentials
        #
        logging.debug("credentials filename is %s" + self.credentials_file)
        try:
            # check for existence of file
            if not Path(self.credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + self.credentials_file +
                    " can not be found!")
            _f = open(self.credentials_file)
            self.credential_file_data = yaml.load(_f, yaml.SafeLoader)
            _f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")
    
    def read_spec_file(self):
        logging.debug("[--- Start read_spec_file ---]")
        logging.debug("load_spec filename is %s" + self.spec_file_name)
        try:
            
            # check for existence of file
            if not Path(self.spec_file_name).is_file():
                sys.exit(
                    "*** load_spec file " + self.spec_file_name + " can not "
                                                                  "be found!")
            f = open(self.spec_file_name)
            self.spec_file_data = yaml.load(f, yaml.SafeLoader)
            self.spec_file_data = {k.lower(): v for k, v in
                                   self.spec_file_data.items()}
            f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")
    
    def connect_cb(self):
        # noinspection PyBroadException
        try:
            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            _f.close()
            
            cluster = Cluster('couchbase://' + _cb_host, ClusterOptions(
                PasswordAuthenticator(_cb_user, _cb_password)))
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            bucket = cluster.bucket('mdata')
            self.collection = bucket.default_collection()
        except:
            logging.error("*** cb_connection Exception failure:  ***",
                          sys.exc_info()[0])
            sys.exit("*** cb_connection Exception failure!")
    
    def runit(self, args):
        """
        This is the entry point for stations_ingest.py
        """
        self.spec_file_name = args['spec_file']
        self.credentials_file = args['credentials_file']
        self.cert_path = None if 'cert_path' not in args.keys() else args[
            'cert_path']
        self.read_spec_file()
        self.connect_cb()
        # process the station data
        self.load_stations()
        # upsert document
        _upsert_start_time = int(time.time())
        logging.info("executing upsert: stop time: " + str(_upsert_start_time))
        self.collection.multi_upsert(self.station_document)
        _upsert_stop_time = int(time.time())
        logging.info("executing upsert: stop time: " + str(_upsert_stop_time))
        logging.info("executing upsert: elapsed time: " + str(
            _upsert_stop_time - _upsert_start_time))
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logging.info("    >>> Total load a_time: %s" + str(load_time))
        logging.info("End a_time: %s" + str(datetime.now()))
        logging.info("--- *** --- End VXStationsIngest --- *** ---")
    
    def main(self):
        args = parse_args(sys.argv)
        self.runit(args)


if __name__ == '__main__':
    VXStationsIngest().main()
