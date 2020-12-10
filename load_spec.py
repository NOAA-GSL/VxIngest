#!/usr/bin/env python3

"""
Program Name: read_load_xml.py
Contact(s): Venita Hagerty
Abstract:
History Log:  Initial version
Usage: Read load_spec XML file
Parameters: N/A
Input Files: load_spec XML file
Output Files: N/A
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
import yaml
from pathlib import Path
import logging
from lxml import etree
import constants as CN


class LoadSpecFile:
    """! Class to read in load_spec file
        file should be eitheran xml file or a yaml file
        Returns:
           N/A
    """

    def __init__(self, specfile):
        # set the defaults
        self.spec_file_name = specfile
        self.load_spec_file_type = 'unknown'
        if specfile.endswith('.xml'):
            self.load_spec_file_type = 'xml'
        if specfile.endswith('.yaml'):
            self.load_spec_file_type = 'yaml'

        self.db_driver = None
        self.insert_size = 1
        self.load_note = None
        self.group = CN.DEFAULT_DATABASE_GROUP
        self.description = "None"
        self.xml_str = None
        self.email = ""
        self.email = ""
        self.initialize_db = ""
        self.organization = ""
        self.connection = {'db_host': None, 'db_port': CN.SQL_PORT, 'db_name': None, 'db_user': None,
                           'db_password': None, 'db_management_system': "mysql"}
        self.flags = {'line_type_load': False, 'load_stat': True, 'load_mode': True, 'load_mtd': True,
                      'load_mpr': False, 'load_orank': False, 'force_dup_file': False, 'verbose': False,
                      'stat_header_db_check': True, 'mode_header_db_check': True, 'mtd_header_db_check': True,
                      'drop_indexes': False, 'apply_indexes': False, 'load_xml': True}

        self.load_files = []
        self.line_types = []

    def read(self):
        """! Read in load_spec file, store values as class attributes
            Returns:
               N/A
        """

        logging.debug("[--- Start read_spec_file ---]")

        try:

            # check for existence of file
            if not Path(self.spec_file_name).is_file():
                sys.exit("*** load_spec file " + self.spec_file_name + " can not be found!")

            # parse the XML file
            if self.load_spec_file_type == 'xml':
                parser = etree.XMLParser(remove_comments=True)
                tree = etree.parse(self.spec_file_name, parser=parser)
                root = tree.getroot()
            if self.load_spec_file_type == 'yaml':
                with open(self.spec_file_name) as f:
                    yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

        folder_template = None
        template_fills = {}
        date_list = {}

        if self.load_spec_file_type == 'xml':
            try:
                # extract values from load_spec XML tags, store in attributes of class XmlLoadFile
                for child in root:
                    if child.tag.lower() == "email":
                        self.email = child.text
                    elif child.tag.lower() == "initialize_db":
                        self.initialize_db = child.text
                    elif child.tag.lower() == "organization":
                        self.organization = child.text
                    elif child.tag.lower() == "connection":
                        for subchild in list(child):
                            if subchild.tag.lower() == "host":
                                host_and_port = subchild.text.split(":")
                            elif subchild.tag.lower() == "user":
                                self.connection['db_user'] = subchild.text
                            elif subchild.tag.lower() == "password":
                                self.connection['db_password'] = subchild.text
                            elif subchild.tag.lower() == "database":
                                self.connection['db_name'] = subchild.text
                            elif subchild.tag.lower() == "management_system":
                                self.connection['db_management_system'] = subchild.text
                        # separate out the port if there is one
                        self.connection['db_host'] = host_and_port[0]
                        if len(host_and_port) > 1:
                            self.connection['db_port'] = int(host_and_port[1])
                        if (not self.connection['db_host']) or (not self.connection['db_name']):
                            logging.warning("!!! XML must include host and database tags")
                        if (not self.connection['db_user']) or (not self.connection['db_password']):
                            logging.warning("!!! XML must include user and passsword tags")
                        if not self.connection['db_name'].startswith("mv_"):
                            logging.warning("!!! Database not visible unless name starts with mv_")
                    elif child.tag.lower() == "load_files":
                        for subchild in list(child):
                            self.load_files.append(subchild.text)
                    elif child.tag.lower() == "folder_tmpl":
                        folder_template = child.text
                    # get the values to fill in to the folder template
                    elif child.tag.lower() == "load_val":
                        for subchild in list(child):
                            template_key = subchild.get("name")
                            template_values = []
                            for template_value in list(subchild):
                                if template_value.tag.lower() == "val":
                                    template_values.append(template_value.text)
                                elif template_value.tag.lower() == "date_list":
                                    template_values.append(template_value.get("name"))
                            template_fills[template_key] = template_values
                    elif child.tag.lower() == "date_list":
                        date_list["name"] = child.get("name")
                        for subchild in list(child):
                            if subchild.tag.lower() == "start":
                                date_list["start"] = subchild.text
                            elif subchild.tag.lower() == "end":
                                date_list["end"] = subchild.text
                            elif subchild.tag.lower() == "inc":
                                date_list["inc"] = subchild.text
                            elif subchild.tag.lower() == "format":
                                date_list["format"] = subchild.text
                    elif child.tag.lower() == "verbose":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['verbose'] = True
                    elif child.tag.lower() == "drop_indexes":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['drop_indexes'] = True
                    elif child.tag.lower() == "apply_indexes":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['apply_indexes'] = True
                    elif child.tag.lower() == "stat_header_db_check":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['stat_header_db_check'] = False
                    elif child.tag.lower() == "mode_header_db_check":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['mode_header_db_check'] = False
                    elif child.tag.lower() == "mtd_header_db_check":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['mtd_header_db_check'] = False
                    elif child.tag.lower() == "load_stat":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['load_stat'] = False
                    elif child.tag.lower() == "load_mode":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['load_mode'] = False
                    elif child.tag.lower() == "load_mtd":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['load_mtd'] = False
                    elif child.tag.lower() == "load_mpr":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['load_mpr'] = True
                    elif child.tag.lower() == "load_orank":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['load_orank'] = True
                    elif child.tag.lower() == "force_dup_file":
                        if child.text.lower() == CN.LC_TRUE:
                            self.flags['force_dup_file'] = True
                    elif child.tag.lower() == "insert_size":
                        if child.text.isdigit():
                            self.insert_size = int(child.text)
                    # group and description for putting databases into groups/categories
                    elif child.tag.lower() == "group":
                        self.group = child.text
                    elif child.tag.lower() == "description":
                        self.description = child.text
                    # load_note and load_xml are used to put a note in the database
                    elif child.tag.lower() == "load_note":
                        self.load_note = child.text
                    elif child.tag.lower() == "load_xml":
                        if child.text.lower() == CN.LC_FALSE:
                            self.flags['load_xml'] = False
                    # MET line types to load. If omitted, all line types are loaded
                    elif child.tag.lower() == "line_type":
                        self.flags['line_type_load'] = True
                        for subchild in list(child):
                            self.line_types.append(subchild.text.upper())
                    else:
                        logging.warning("!!! Unknown tag: %s", child.tag)

                # if requested, get a string of the XML to put in the database
                if self.flags['load_xml']:
                    self.xml_str = etree.tostring(tree).decode().replace('\n', '').replace(' ', '')

            except (RuntimeError, TypeError, NameError, KeyError):
                logging.error("*** %s in read xml ***", sys.exc_info()[0])
                sys.exit("*** Error(s) found while reading XML file!")
        else:
            try:
                yaml.dump(yaml_data)
                # lower case all the keys of the yaml_data
                yaml_data((k.lower(), v) for k, v in {'My Key': 'My Value'}.iteritems())
                self.connection['db_user'] = yaml_data['load_spec']['connection']['user']
                self.connection['db_password'] = yaml_data['load_spec']['connection']['password']
                self.connection['db_name'] = yaml_data['load_spec']['connection']['database']
                self.connection['db_management_system'] = yaml_data['load_spec']['connection']['management_system']
                host_and_port = yaml_data['load_spec']['connection']['host'].split(":")
                self.connection['db_host'] = host_and_port[0]
                if host_and_port[1]:
                    self.connection['db_port'] = int(host_and_port[1])
                if (not self.connection['db_host']) or (not self.connection['db_name']):
                    logging.warning("!!! XML must include host and database tags")
                if (not self.connection['db_user']) or (not self.connection['db_password']):
                    logging.warning("!!! XML must include user and passsword tags")
                if not self.connection['db_name'].startswith("mv_"):
                    logging.warning("!!! Database not visible unless name starts with mv_")
                if 'load_files' in yaml_data['load_spec'].keys():
                    self.load_files = yaml_data['load_spec']['load_files']
                folder_template = yaml_data['load_spec']['folder_tmpl']

                # get the values to fill in to the folder template
                if 'load_val' in yaml_data['load_spec'].keys():
                    for subchild in list(child):
                        template_key = subchild.get("name")
                        template_values = []
                        for template_value in list(subchild):
                            if template_value.tag.lower() == "val":
                                template_values.append(template_value.text)
                            elif template_value.tag.lower() == "date_list":
                                template_values.append(template_value.get("name"))
                        template_fills[template_key] = template_values

                elif child.tag.lower() == "date_list":
                    date_list["name"] = child.get("name")
                    for subchild in list(child):
                        if subchild.tag.lower() == "start":
                            date_list["start"] = subchild.text
                        elif subchild.tag.lower() == "end":
                            date_list["end"] = subchild.text
                        elif subchild.tag.lower() == "inc":
                            date_list["inc"] = subchild.text
                        elif subchild.tag.lower() == "format":
                            date_list["format"] = subchild.text

                self.flags['verbose'] = yaml_data['load_spec']['verbose']
                self.flags['drop_indexes'] = yaml_data['load_spec']['drop_indexes']
                self.flags['apply_indexes'] = yaml_data['load_spec']['apply_indexes']
                self.flags['stat_header_db_check'] = yaml_data['load_spec']['stat_header_db_check']
                self.flags['mode_header_db_check'] = yaml_data['load_spec']['mode_header_db_check']
                self.flags['mtd_header_db_check'] = yaml_data['load_spec']['mtd_header_db_check']
                self.flags['load_stat'] = yaml_data['load_spec']['load_stat']
                self.flags['load_mode'] = yaml_data['load_spec']['load_mode']
                self.flags['load_mtd'] = yaml_data['load_spec']['load_mtd']
                self.flags['load_mpr'] = yaml_data['load_spec']['load_mpr']
                self.flags['load_orank'] = yaml_data['load_spec']['load_orank']
                self.flags['force_dup_file'] = yaml_data['load_spec']['force_dup_file']
                self.insert_size = yaml_data['load_spec']['insert_size']

                # group and description for putting databases into groups/categories
                self.group = yaml_data['load_spec']['group']
                self.description = yaml_data['load_spec']['description']
                # load_note and load_xml are used to put a note in the database
                self.load_note = yaml_data['load_spec']['load_note']
                self.flags['load_xml'] = yaml_data['load_spec']['load_xml']
                self.flags['line_type_load'] = yaml_data['load_spec']['line_type_load']
                self.line_types = yaml_data['load_spec']['line_types']
                date_list = yaml_data['load_spec']['date_list']
                template_fills = yaml_data['load_spec']['load_val']
                # if requested, get a string of the XML to put in the database
                if self.flags['load_xml']:
                    self.xml_str = str(yaml_data)
            except:
                logging.error("*** %s in read yaml ***", sys.exc_info()[0])
                sys.exit("*** Error(s) found while reading YAML file!")

        logging.debug("db_name is: %s", self.connection['db_name'])
        # this removes duplicate file names. do we want that?
        if self.load_files is not None:
            self.load_files = list(dict.fromkeys(self.load_files))
        # remove directory names
        self.load_files = [lf for lf in self.load_files if '.' in lf.split('/')[-1]]
        logging.debug("Initial number of files: %s", str(len(self.load_files)))
        logging.debug("[--- End read ---]")

    @staticmethod
    def filenames_from_date(date_list):
        """! given date format, start and end dates, and increment, generates list of dates
            Returns:
               list of dates
        """
        logging.debug("date format is: %s", date_list["format"])

        try:
            date_format = date_list["format"]
            # check to make sure that the date format string only has known characters
            if set(date_format) <= CN.DATE_CHARS:
                # Change the java formatting string to a Python formatting string
                for java_date, python_date in CN.DATE_SUBS.items():
                    date_format = date_format.replace(java_date, python_date)
                # format the start and end dates
                date_start = pd.to_datetime(date_list["start"], format=date_format)
                date_end = pd.to_datetime(date_list["end"], format=date_format)
                date_inc = int(date_list["inc"])
                all_dates = []
                while date_start < date_end:
                    all_dates.append(date_start.strftime(date_format))
                    date_start = date_start + pd.Timedelta(seconds=date_inc)
                all_dates.append(date_end.strftime(date_format))
            else:
                logging.error("*** date_list tag has unknown characters ***")

        except ValueError as value_error:
            logging.error("*** %s in filenames_from_date ***", sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML date format!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_date ***", sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML date format!")

        return all_dates

    @staticmethod
    def filenames_from_template(folder_template, template_fills):
        """! given a folder template and the values to fill in, generates list of filenames
            Returns:
               list of filenames
        """
        logging.debug("folder template is: %s", folder_template)

        try:

            fills_open = folder_template.count("{")
            if fills_open != folder_template.count("}"):
                raise ValueError("mismatched curly braces")
            # remove any fill values that are not in the template
            if template_fills:
                copy_template_fills = dict(template_fills)
                for key in copy_template_fills:
                    if key not in folder_template:
                        del template_fills[key]
            if fills_open > len(template_fills):
                raise ValueError("not enough template fill values")
            # generate a list of directories with all combinations of values filled in
            load_dirs = [folder_template]
            for key in template_fills:
                alist = []
                for fvalue in template_fills[key]:
                    for tvalue in load_dirs:
                        alist.append(tvalue.replace("{" + key + "}", fvalue))
                load_dirs = alist
            # find all files in directories, append path to them, and put on load_files list
            file_list = []
            for file_dir in load_dirs:
                if os.path.exists(file_dir):
                    for file_name in os.listdir(file_dir):
                        file_list.append(file_dir + "/" + file_name)

        except ValueError as value_error:
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML folder templates!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_template ***", sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML folder templates!")

        return file_list
