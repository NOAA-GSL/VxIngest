#!/usr/bin/env python3

"""
Program Name: read_load_yaml.py
Contact(s): Randy Pierce
Abstract:
History Log:  Initial version
Usage: Read load_spec YAML file
Parameters: N/A
Input Files: load_spec YAML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import logging
import sys
from pathlib import Path

import yaml


class LoadYamlSpecFile:
    """! Class to read in load_spec file
        file should be a yaml file
        Returns:
           N/A
    """
    
    def __init__(self, args):
        # set the defaults
        # args requires {'spec_file':something, ['spec_type':'gsd_builder'] }
        self.spec_file_name = args['spec_file']
        self.connection_list = ['cb_connection', 'mysql_connection']
        self.load_spec = {'cb_connection': {'host': None, 'user': None, 'password': None, },
                          'mysql_connection': {
            'host': None, 'password': None, }, 'email': None, 'ingest_document_ids': []}
        self.yaml_data = {}
    
    def read(self):
        """! Read in load_spec file, store values as class attributes
            Returns:
               N/A
        """
        
        logging.debug("[--- Start read_spec_file ---]")
        
        try:
            
            # check for existence of file
            if not Path(self.spec_file_name).is_file():
                sys.exit("*** load_spec file " + self.spec_file_name + " can not "
                                                                       "be found!")
            f = open(self.spec_file_name)
            self.yaml_data = yaml.load(f, yaml.SafeLoader)
            self.yaml_data = {k.lower(): v for k, v in self.yaml_data.items()}
            f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")
        
        # noinspection PyBroadException
        try:
            # process  yaml file
            # yaml.dump(self.yaml_data)
            # deal with connections
            for c_key in self.connection_list:
                c_keys = self.load_spec[c_key].keys()
                for k in c_keys:
                    try:
                        # assign the connection keys from the yaml_data
                        if k in self.load_spec[c_key].keys():
                            self.load_spec[c_key][k] = self.yaml_data['load_spec'][c_key][k]
                    except KeyError:
                        logging.warning(
                            "yaml file: " + self.spec_file_name + " is missing key: load_spec[" + c_key + "]['" + k +
                            "'] - using default")
                # assign the top level keys
                for k in self.yaml_data['load_spec'].keys():
                    self.load_spec[k] = self.yaml_data['load_spec'][k]
        
        except:
            logging.error("*** %s in read yaml ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading YAML file!")
        
        logging.debug("[--- End read ---]")
        
        return self.load_spec
