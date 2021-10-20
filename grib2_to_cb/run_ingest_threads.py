"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -s spec_file -c credentials_file -p path -m _file_mask[-o output_dir -t thread_count -f first_epoch -l last_epoch -n number_stations]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
and a thread count.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path, mask, first_epoch, and last_epoch parameters.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
The optional -n number_stations will restrict the processing to n number of stations to limit run time.
Each thread will run a VxIngestManager which will pull filenames, one at a time,
from the filename queue and fully process that input file.
When the queue is empty each NetcdfIngestManager will gracefully die.
Only files that do not have a DataFile entry in the database will be added to the file queue.
When a file is processed it a datafile entry will be made for that file and added to the result documents to ne imported.
This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_id: 'MD:V01:METAR:obs'
  cb_connection:
    management_system: cb
    host: "cb_host"   - should come from defaults file
    user: "cb_user"   - should come from defaults file
    password: "cb_pwd" - should come from defaults file

The mask  is a python time.strftime format e.g. '%y%j%H%f',
The optional output_dir specifies the directory where output files will be written instead
of writing them directly to couchbase. If the output_dir is not specified data will be written
to couchbase cluster specified in the cb_connection.
Files in the path will be enqueued if the file name mask renders a valid datetime that
falls between the first_epoch and the last_epoch.
The first_epoch and the last_epoch may be omitted in which case all the non processed files in the path
will be processed.

This is an example defaults file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name

This is an example invocation in bash. t=The python must be python3.
export PYTHONPATH=${HOME}/VXingest
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /data/grib2_to_cb/input_files -m %y%j%H%f -o /data/grib2_to_cb/output


Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue
from pathlib import Path
import json
import yaml
from couchbase.cluster import Cluster, ClusterOptions, ClusterTracingOptions
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.load_spec_yaml import LoadYamlSpecFile
from grib2_to_cb.vx_ingest_manager import VxIngestManager

def parse_args(args):
    """
    Parse command line arguments
    """
    begin_time = str(datetime.now())
    logging.getLogger().setLevel(logging.INFO)
    logging.info("--- *** --- Start --- *** ---")
    logging.info("Begin a_time: %s", begin_time)
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--spec_file", type=str,
                        help="Please provide required load_spec filename "
                             "-s something.xml or -s something.yaml")
    parser.add_argument("-c", "--credentials_file", type=str,
                        help="Please provide required credentials_file")
    parser.add_argument("-t", "--threads", type=int, default=1,
                        help="Number of threads to use")
    parser.add_argument("-p", "--path", type=str, default="./",
                        help="Specify the input directory that contains the input files")
    parser.add_argument("-m", "--file_name_mask", type=str, default="%Y%m%d_%H%M",
                        help="Specify the file name mask for the input files ()")
    parser.add_argument("-o", "--output_dir", type=str, default="/tmp",
                        help="Specify the output directory to put the json output files")
    parser.add_argument("-f", "--first_epoc", type=int, default=0,
                        help="The first epoch to use, inclusive")
    parser.add_argument("-l", "--last_epoch", type=int, default=sys.maxsize,
                        help="The last epoch to use, exclusive")
    parser.add_argument("-n", "--number_stations", type=int, default=sys.maxsize,
                        help="The maximum number of stations to process")
    # get the command line arguments
    args = parser.parse_args(args)
    return args

class VXIngest(object):
    """
    This class is the commandline mechanism for using the grib2_to_cb builder.
    This class will maintain the couchbase collection and cluster objects for all
    the ingest managers that this thread will use. There will be VxIngestManagers started
    to match the threadcount that is passed in. The default number of threads is one.
    """
    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.spec_file = ""
        self.credentials_file = ""
        self.thread_count = ""
        # -f first_epoch and -l last_epoch are optional time params.
        # If these are present only the files in the path with filename masks
        # that fall between these epochs will be processed.
        self.first_last_params = {'first_epoch': 0,'last_epoch': sys.maxsize}
        self.path = None
        self.fmask = None
        self.output_dir = None
        # optional: used to limit the number of stations processed
        self.number_stations = sys.maxsize
        self.load_job_id = None
        self.load_spec = None
        self.cb_credentials = None
        self.collection = None
        self.cluster = None
        self.ingest_document_id = None
        self.ingest_document = None
        logging.getLogger().setLevel(logging.INFO)

    def write_load_job_to_files(self):
        # The document_map is all built now so write all the
        # documents in the document_map into files in the output_dir
        # noinspection PyBroadException
        try:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
            try:
                file_name = self.load_job_id + ".json"
                complete_file_name = os.path.join(self.output_dir, file_name)
                f = open(complete_file_name, "w")
                f.write(json.dumps([self.load_spec['load_job_doc']]))
                f.close()
            except Exception as e:
                logging.info("process_file - trying write load_job: Got Exception - %s", str(e))
        except Exception as e:
            logging.error(": *** Error writing load_job to files: %s***", str(e))
            raise e

    def build_load_job_doc(self):
        """
        This method will build a load_job document for GribBuilder
        """
        self.load_job_id = "LF:{m}:{c}:{t}".format(m=self.__module__, c=self.__class__.__name__, t=str(int(time.time())))
        stream = os.popen('git rev-parse HEAD')
        git_hash = stream.read().strip()
        lj_doc = {
            "id": self.load_job_id,
            "subset": "metar",
            "type": "DF",
            "lineageId": "",
            "script": "__file__",
            "scriptVersion": git_hash,
            "loadSpec": self.spec_file,
            "note": ""
        }
        return lj_doc

    def close_cb(self):
        """
            close couchbase connection
        """
        if self.cluster:
            self.cluster.disconnect()

    def connect_cb(self):
        """
        create a couchbase connection and maintain the collection and cluster objects.
        """
        logging.info('%s: data_type_manager - Connecting to couchbase')
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(self.cb_credentials['user'], self.cb_credentials['password']))
            self.cluster = Cluster(
                'couchbase://' + self.cb_credentials['host'], options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            logging.info('%s: Couchbase connection success')
        except Exception as e:
            logging.error("*** %s in connect_cb ***", str(e))
            sys.exit("*** Error when connecting to mysql database: ")

    def runit(self, args):
        """
        This is the entry point for run_ingest_threads.py
        """
        self.spec_file = args['spec_file'].strip()
        self.credentials_file = args['credentials_file'].strip()
        self.path = args['path'].strip()
        self.fmask = args['file_name_mask'].strip()
        self.thread_count = args['threads']
        self.output_dir = args['output_dir'].strip()
        _args_keys = args.keys()
        if 'first_epoch' in _args_keys:
            self.first_last_params['first_epoch'] = args['first_epoch']
        if 'last_epoch' in _args_keys:
            self.first_last_params['last_epoch'] = args['last_epoch']
        if 'number_stations' in _args_keys:
            self.number_stations = args['number_stations']
        else:
            self.number_stations = sys.maxsize
        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s", self.spec_file)
            load_spec_file = LoadYamlSpecFile({'spec_file': self.spec_file})
            # read in the load_spec file
            self.load_spec = dict(load_spec_file.read())
            # put the real credentials into the load_spec
            self.cb_credentials = self.get_credentials(self.load_spec)
            # stash the first_last_params because the builder will need to detrmine
            # if it needs to check for the latest validEpoch from the database (first_epoch == 0)
            self.load_spec['first_last_params'] = self.first_last_params
            # stash the load_job
            self.load_spec['load_job_doc'] = self.build_load_job_doc()
            # get the ingest document id.
            # NOTE: in future we may make this (ingest_document_id) a list
            # and start each VxIngestManager with its own ingest_document_id
            self.ingest_document_id = self.load_spec['ingest_document_id']
            # establish connections to cb, collection
            self.connect_cb()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** Error occurred in Main reading load_spec %s: %s ***",
                self.spec_file, str(sys.exc_info()))
            sys.exit("*** Error reading load_spec: " + self.spec_file)

        ingest_document = self.collection.get(self.ingest_document_id).content
        # load the my_queue with filenames that match the mask and have not already been ingested
        # (do not have associated datafile documents)
        # Constructor for an infinite size  FIFO my_queue
        q = JoinableQueue()
        file_names = []
        model = ingest_document['model']
        # get the urls (full_file_names) from all the datafiles for this type of ingest
        result = self.cluster.query("""
        SELECT url
        FROM mdata
        WHERE
        subset='metar'
        AND type='DF'
        AND fileType='grib2'
        AND originType='model'
        AND model='{model}';
        """.format(model=model))
        df_full_names = list(result)
        if os.path.exists(self.path) and os.path.isdir(self.path):
            with os.scandir(self.path) as entries:
                for entry in entries:
                    # convert the file name to an epoch using the mask
                    # first remove any characters from the file_name that correpond
                    # to "|" in the mask. Also remove the "|" characters from the mask itself
                    try:
                        #entry_name = entry.name
                        #file_utc_time = datetime.strptime(entry_name, self.fmask)
                        #file_time = int((file_utc_time - datetime(1970, 1, 1)).total_seconds())
                        # check to see if this file has already been processed (if it is in the df_full_names)
                        if  entry.name not in df_full_names:
                            file_names.append(entry.path)
                    except:
                        # don't care, it just means it wasn't a properly formatted file per the mask
                        continue
        if len(file_names) == 0:
            raise Exception("No files to Process!")
        for f in file_names:
            q.put(f)

        # instantiate ingest_manager pool - each ingest_manager is a process
        # thread that uses builders to process one file at a time from the queue
        # Make the Pool of ingest_managers
        ingest_manager_list = []
        for thread_count in range(int(self.thread_count)):
            # noinspection PyBroadException
            try:
                self.load_spec['fmask'] = self.fmask
                # passing a cluster and collection is giving me trouble so each VxIngestManager is getting its own, for now.
                ingest_manager_thread = VxIngestManager(
                    "VxIngestManager-" + str(thread_count), self.load_spec, ingest_document, q, self.output_dir, number_stations=self.number_stations)
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except:
                logging.error("*** Error in  VxIngestManager %s***", str(sys.exc_info()))
        # be sure to join all the threads to wait on them
        finished = [proc.join() for proc in ingest_manager_list]
        self.write_load_job_to_files()
        logging.info("finished starting threads")
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logging.info(" finished %s", str(finished))
        logging.info("    >>> Total load a_time: %s", str(load_time))
        logging.info("End a_time: %s", str(datetime.now()))
        logging.info("--- *** --- End  --- *** ---")

    def get_credentials(self, load_spec):
        #
        #  Read the credentials
        #
        logging.debug("credentials filename is %s", self.credentials_file)
        try:
            # check for existence of file
            if not Path(self.credentials_file).is_file():
                sys.exit("*** credentials_file file " +
                         self.credentials_file + " can not be found!")
            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            load_spec['cb_connection']['host'] = yaml_data['cb_host']
            load_spec['cb_connection']['user'] = yaml_data['cb_user']
            load_spec['cb_connection']['password'] = yaml_data['cb_password']
            f.close()
            return load_spec['cb_connection']
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

    def main(self):
        """run_ingest_threads main entry. Manages a set of VxIngestManagers for processing grib files"""
        logging.info("PYTHONPATH: %s", os.environ['PYTHONPATH'])
        args = parse_args(sys.argv[1:])
        self.runit(vars(args))
        sys.exit("*** FINISHED ***")


if __name__ == '__main__':
    VXIngest().main()
