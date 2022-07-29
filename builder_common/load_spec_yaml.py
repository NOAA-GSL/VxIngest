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
import json
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
        self.spec_file_name = args["spec_file"].lstrip()
        self.load_spec = {
            "email": None,
            "ingest_document_ids": [],
        }
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
                sys.exit(
                    "*** load_spec file " + self.spec_file_name + " can not be found!"
                )
            _f = open(self.spec_file_name)
            self.yaml_data = yaml.load(_f, yaml.SafeLoader)
            self.yaml_data = {k.lower(): v for k, v in self.yaml_data.items()}
            _f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

        # noinspection PyBroadException
        try:
            # process  yaml file
            # yaml.dump(self.yaml_data)
            for k in self.yaml_data["load_spec"].keys():
                self.load_spec[k] = self.yaml_data["load_spec"][k]
                # log message for scraping
                if k.startswith("ingest_document_id"):
                    if k.startswith("ingest_document_ids"):
                        # plural case - convert to list and iterate
                        id_list = json.loads(self.yaml_data["load_spec"][k])
                        for _l in id_list:
                            logging.info("LoadYamlSpecFile ingest_document_id %s", self.yaml_data["load_spec"][k][_l])
                    else:
                        # singular case
                        logging.info("LoadYamlSpecFile ingest_document_id %s", self.yaml_data["load_spec"][k])

        except Exception:  # pylint: disable=bare-except, disable=broad-except
            logging.error("*** %s in read yaml ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading YAML file!")

        logging.debug("[--- End read ---]")

        return self.load_spec
