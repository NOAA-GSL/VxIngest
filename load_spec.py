import sys
import os
from pathlib import Path
import logging
import yaml

import constants as cn


class LoadSpecFile:
    """! Class to read in load_spec xml or yaml file
        Returns:
           N/A
    """

    def __init__(self, file_name):
        # set the defaults
        self.filename = file_name

        self.connection = {cn.DB_HOST: None, cn.DB_PORT: cn.SQL_PORT, cn.DB_NAME: None, cn.DB_USER: None,
                           'db_password': None, 'db_management_system': "mysql"}

        self.db_driver = None
        self.insert_size = 1
        self.load_note = None
        self.group = cn.DEFAULT_DATABASE_GROUP
        self.description = "None"
        self.xml_str = None

        self.flags = {'line_type_load': False, 'load_stat': True, 'load_mode': True, 'load_mtd': True,
                      'load_mpr': False, 'load_orank': False, 'force_dup_file': False, 'verbose': False,
                      'stat_header_db_check': True, 'mode_header_db_check': True, 'mtd_header_db_check': True,
                      'drop_indexes': False, 'apply_indexes': False, 'load_xml': True}

        self.load_files = []
        self.line_types = []

    def __init__(self, spec_file):
        # set the defaults
        self.spec_file_name = spec_file

        self.connection = {'db_host': None, 'db_port': cn.SQL_PORT, 'db_name': None, 'db_user': None,
                           'db_password': None, 'db_management_system': "mysql"}

        self.db_driver = None
        self.insert_size = 1
        self.load_note = None
        self.group = cn.DEFAULT_DATABASE_GROUP
        self.description = "None"
        self.xml_str = None

        self.flags = {'line_type_load': False, 'load_stat': True, 'load_mode': True, 'load_mtd': True,
                      'load_mpr': False, 'load_orank': False, 'force_dup_file': False, 'verbose': False,
                      'stat_header_db_check': True, 'mode_header_db_check': True, 'mtd_header_db_check': True,
                      'drop_indexes': False, 'apply_indexes': False, 'load_xml': True}

        self.load_files = []
        self.line_types = []
