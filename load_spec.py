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
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

# pylint:disable=no-member
# constants exist in constants.py

import sys
import os
import yaml
from pathlib import Path
import logging
from lxml import etree
import constants as cn
import pandas as pd


class LoadSpecFile:
    """! Class to read in load_spec file
        file should be eitheran xml file or a yaml file
        Returns:
           N/A
    """
    
    def __init__(self, args):
        # set the defaults
        # args requires {'spec_file':something, ['spec_type':'gsd'] }
        self.spec_file_name = args['spec_file']
        self.load_spec_file_type = 'unknown'
        self.spec_type = "met"
        self.root = None
        self.host_and_port = None
        self.tree = None
        self.folder_template = None
        self.template_fills = {}
        self.date_list = {}
        
        if self.spec_file_name.endswith('.xml'):
            self.load_spec_file_type = 'xml'
        if self.spec_file_name.endswith('.yaml'):
            self.load_spec_file_type = 'yaml'
        if 'spec_type' in args.keys() and args['spec_type'].lower() == 'gsd':
            # define and initialize a gsd style load_spec
            self.spec_type = "gsd"
            self.connection_list = ['cb_connection', 'mysql_connection']
            self.load_spec = {
                'cb_connection': {'host': None,
                                  'user': None, 'password': None,
                                  'management_system': "cb"},
                'mysql_connection': {'host': None,
                                     'database': None, 'user': None,
                                     'password': None,
                                     'management_system': "mysql"},
                'email': None, 'ingest_document_ids': []}
        else:
            # It isn't gsd_spec so define and initialize a met style load_spec
            self.connection_list = ['connection']
            self.load_spec = {'connection': {'host': None,
                                             'database': None, 'user': None,
                                             'password': None,
                                             'management_system': "mysql"},
                              'flags': {'line_type_load': False,
                                        'load_stat': True, 'load_mode': True,
                                        'load_mtd': True, 'load_mpr': False,
                                        'load_orank': False,
                                        'force_dup_file': False,
                                        'verbose': False,
                                        'stat_header_db_check': True,
                                        'mode_header_db_check': True,
                                        'mtd_header_db_check': True,
                                        'drop_indexes': False,
                                        'apply_indexes': False,
                                        'load_xml': True}, 'db_driver': None,
                              'insert_size': 1, 'load_note': None,
                              'group': cn.DEFAULT_DATABASE_GROUP,
                              'description': None, 'xml_str': None,
                              'email': None, 'initialize_db': None,
                              'organization': None, 'load_files': [],
                              'line_types': [], 'folder_tmpl': None}
        
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
                    "*** load_spec file " + self.spec_file_name + " can not "
                                                                  "be found!")
            
            # parse the XML file  (this is largely copied directly from
            # met_db_load)
            if self.load_spec_file_type == 'xml':
                parser = etree.XMLParser(remove_comments=True)
                self.tree = etree.parse(self.spec_file_name, parser=parser)
                self.root = self.tree.getroot()
            if self.load_spec_file_type == 'yaml':
                f = open(self.spec_file_name)
                self.yaml_data = yaml.load(f, yaml.SafeLoader)
                self.yaml_data = {k.lower(): v for k, v in
                                  self.yaml_data.items()}
                f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")
        
        if self.load_spec_file_type == 'xml':
            # process met xml file - only met has xml for now
            try:
                # extract values from load_spec XML tags, store in
                # attributes of class XmlLoadFile
                for child in self.root:
                    if child.tag.lower() == "email":
                        self.load_spec['email'] = child.text
                    elif child.tag.lower() == "initialize_db":
                        self.load_spec['initialize_db'] = child.text
                    elif child.tag.lower() == "organization":
                        self.load_spec['organization'] = child.text
                    elif child.tag.lower() == "connection":
                        for sub_child in list(child):
                            if sub_child.tag.lower() == "host":
                                self.host_and_port = sub_child.text.split(":")
                            elif sub_child.tag.lower() == "user":
                                self.load_spec['connection'][
                                    'user'] = sub_child.text
                            elif sub_child.tag.lower() == "password":
                                self.load_spec['connection'][
                                    'password'] = sub_child.text
                            elif sub_child.tag.lower() == "database":
                                self.load_spec['connection'][
                                    'database'] = sub_child.text
                            elif sub_child.tag.lower() == "management_system":
                                self.load_spec['connection'][
                                    'management_system'] = sub_child.text
                        # separate out the port if there is one
                        self.load_spec['connection']['host'] = \
                            self.host_and_port[0]
                        if len(self.host_and_port) > 1:
                            self.load_spec['connection']['port'] = int(
                                self.host_and_port[1])
                        if (not self.load_spec['connection']['host']) or (
                                not self.load_spec['connection']['database']):
                            logging.warning(
                                "!!! load_spec must include host and "
                                "database tags")
                        if (not self.load_spec['connection']['user']) or (
                                not self.load_spec['connection']['password']):
                            logging.warning(
                                "!!! load_spec must include user and "
                                "password tags")
                        if not self.load_spec['connection']['database'].\
                                startswith("mv_"):
                            logging.warning(
                                "!!! Database not visible unless name starts "
                                "with mv_")
                    elif child.tag.lower() == "load_files":
                        for sub_child in list(child):
                            self.load_spec['load_files'].append(sub_child.text)
                    elif child.tag.lower() == "folder_tmpl":
                        self.folder_template = child.text
                    # get the values to fill in to the folder template
                    elif child.tag.lower() == "load_val":
                        for sub_child in list(child):
                            template_key = sub_child.get("name")
                            template_values = []
                            for template_value in list(sub_child):
                                if template_value.tag.lower() == "val":
                                    template_values.append(template_value.text)
                                elif template_value.tag.lower() == "date_list":
                                    template_values.append(
                                        template_value.get("name"))
                            self.template_fills[template_key] = template_values
                    elif child.tag.lower() == "date_list":
                        self.date_list["name"] = child.get("name")
                        for sub_child in list(child):
                            if sub_child.tag.lower() == "start":
                                self.date_list["start"] = sub_child.text
                            elif sub_child.tag.lower() == "end":
                                self.date_list["end"] = sub_child.text
                            elif sub_child.tag.lower() == "inc":
                                self.date_list["inc"] = sub_child.text
                            elif sub_child.tag.lower() == "format":
                                self.date_list["format"] = sub_child.text
                    elif child.tag.lower() == "verbose":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['verbose'] = True
                    elif child.tag.lower() == "drop_indexes":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['drop_indexes'] = True
                    elif child.tag.lower() == "apply_indexes":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['apply_indexes'] = True
                    elif child.tag.lower() == "stat_header_db_check":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags'][
                                'stat_header_db_check'] = False
                    elif child.tag.lower() == "mode_header_db_check":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags'][
                                'mode_header_db_check'] = False
                    elif child.tag.lower() == "mtd_header_db_check":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags'][
                                'mtd_header_db_check'] = False
                    elif child.tag.lower() == "load_stat":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags']['load_stat'] = False
                    elif child.tag.lower() == "load_mode":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags']['load_mode'] = False
                    elif child.tag.lower() == "load_mtd":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags']['load_mtd'] = False
                    elif child.tag.lower() == "load_mpr":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['load_mpr'] = True
                    elif child.tag.lower() == "load_orank":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['load_orank'] = True
                    elif child.tag.lower() == "force_dup_file":
                        if child.text.lower() == cn.LC_TRUE:
                            self.load_spec['flags']['force_dup_file'] = True
                    elif child.tag.lower() == "insert_size":
                        if child.text.isdigit():
                            self.load_spec['insert_size'] = int(child.text)
                    # group and description for putting databases into
                    # groups/categories
                    elif child.tag.lower() == "group":
                        self.load_spec['group'] = child.text
                    elif child.tag.lower() == "description":
                        self.load_spec['description'] = child.text
                    # load_note and load_xml are used to put a note in the
                    # database
                    elif child.tag.lower() == "load_note":
                        self.load_spec['load_note'] = child.text
                    elif child.tag.lower() == "load_xml":
                        if child.text.lower() == cn.LC_FALSE:
                            self.load_spec['flags']['load_xml'] = False
                    # MET line types to load. If omitted, all line types are
                    # loaded
                    elif child.tag.lower() == "line_type":
                        self.load_spec['flags']['line_type_load'] = True
                        for sub_child in list(child):
                            self.load_spec['line_types'].append(
                                sub_child.text.upper())
                    else:
                        logging.warning("!!! Unknown tag: %s", child.tag)
                
                # if requested, get a string of the XML to put in the database
                if self.load_spec['flags']['load_xml']:
                    self.load_spec['xml_str'] = etree.tostring(
                        self.tree).decode().replace('\n', '').replace(' ', '')
                
                self.load_spec['load_files'] = self.filenames_from_template(
                    self.folder_template, self.template_fills)
            
            except (RuntimeError, TypeError, NameError, KeyError):
                logging.error("*** %s in read xml ***", sys.exc_info()[0])
                sys.exit("*** Error(s) found while reading XML file!")
        else:  # this is a yaml spec file - COULD BE GSD OR MET
            try:
                # process  yaml file
                # yaml.dump(self.yaml_data)
                # deal with connections - both met or gsd
                for c_key in self.connection_list:
                    c_keys = self.load_spec[c_key].keys()
                    for k in c_keys:
                        try:
                            # assign the connection keys from the yaml_data
                            if k in self.load_spec[c_key].keys():
                                self.load_spec[c_key][k] = \
                                    self.yaml_data['load_spec'][c_key][k]
                            if k == 'host':
                                #  deal with the possibility of a host:port
                                #  string
                                self.host_and_port = \
                                    self.yaml_data['load_spec'][c_key][
                                        'host'].split(":")
                                self.load_spec[c_key]['host'] = \
                                    self.host_and_port[0]
                                # assign port if it is included
                                if len(self.host_and_port) > 1:
                                    self.load_spec[c_key]['port'] = int(
                                        self.host_and_port[1])
                        except KeyError:
                            logging.warning(
                                "yaml file: " +
                                self.spec_file_name +
                                " is missing key: load_spec[" + c_key + "]['" +
                                k + "'] - using default")
                    
                    # assign the top level keys - both met and gsd
                    for k in self.load_spec.keys():
                        if k in [c_key, 'flags']:
                            continue
                        try:
                            self.load_spec[k] = self.yaml_data['load_spec'][k]
                            if k == 'folder_tmpl':
                                self.folder_template = \
                                    self.yaml_data['load_spec'][k]
                        except KeyError:
                            logging.warning(
                                "yaml file: " + self.spec_file_name +
                                " is missing key: load_spec['" +
                                k + "'] " + "- using default")
                    
                    # met specific validation and file loading
                    if not self.spec_type == 'gsd':
                        #  met spec, we have to verify we have to validate
                        #  the connection parameters
                        if (not self.load_spec[c_key]['host']) or (
                                not self.load_spec[c_key]['database']):
                            logging.error(
                                "!!! XML must include host and database tags")
                            sys.exit(
                                "*** Error(s) found while reading XML file! "
                                "!!! " + "XML must include host and database "
                                         "tags")
                        if (not self.load_spec[c_key]['user']) or (
                                not self.load_spec[c_key]['password']):
                            logging.error(
                                "!!! XML must include user and password tags")
                            sys.exit(
                                "*** Error(s) found while reading XML file! "
                                "!!! " + "XML must include user and password "
                                         "tags")
                        if not self.load_spec[c_key]['database'].startswith(
                                "mv_"):
                            logging.error(
                                "!!! Database not visible unless name starts "
                                "with mv_")
                            sys.exit(
                                "*** Error(s) found while reading XML file! "
                                "!!! " + "Database not visible unless name "
                                         "starts with mv_")
                        
                        # get the flags values to fill in to the
                        # load_spec_file - only for met specs
                        for k in self.load_spec['flags'].keys():
                            try:
                                self.load_spec['flags'][k] = \
                                    self.yaml_data['load_spec'][k]
                            except KeyError:
                                logging.warning(
                                    "yaml file: " +
                                    self.spec_file_name +
                                    " is missing key: load_spec['flags']['" +
                                    k + "'] - using default")
                        
                        # met only - date_list = self.yaml_data[
                        # 'load_spec']['date_list']
                        self.template_fills = self.yaml_data['load_spec'][
                            'load_val']
                        # met only - if requested, get a string of the XML
                        # to put in the database
                        if 'flags' in self.load_spec.keys() and 'load_xml' in \
                                self.load_spec['flags'].keys() and \
                                self.load_spec['flags']['load_xml'] is True:
                            self.load_spec['xml_str'] = str(self.yaml_data)
                        
                        self.load_spec[
                            'load_files'] = self.filenames_from_template(
                            self.folder_template, self.template_fills)
            except:
                logging.error("*** %s in read yaml ***", sys.exc_info()[0])
                logging.error("*** %s in read yaml ***", sys.exc_info()[1])
                sys.exit("*** Error(s) found while reading YAML file!")
            
            logging.debug("[--- End read ---]")
        
        return self.load_spec
    
    @staticmethod
    def filenames_from_date(date_list):
        """! given date format, start and end dates, and increment,
        generates list of dates
            Returns:
               list of dates
        """
        logging.debug("date format is: %s", date_list["format"])
        
        all_dates = []
        try:
            date_format = date_list["format"]
            # check to make sure that the date format string only has known
            # characters
            if set(date_format) <= cn.DATE_CHARS:
                # Change the java formatting string to a Python formatting
                # string
                for java_date, python_date in cn.DATE_SUBS.items():
                    date_format = date_format.replace(java_date, python_date)
                # format the start and end dates
                date_start = pd.to_datetime(date_list["start"],
                                            format=date_format)
                date_end = pd.to_datetime(date_list["end"], format=date_format)
                date_inc = int(date_list["inc"])
                while date_start < date_end:
                    all_dates.append(date_start.strftime(date_format))
                    date_start = date_start + pd.Timedelta(seconds=date_inc)
                all_dates.append(date_end.strftime(date_format))
            else:
                logging.error("*** date_list tag has unknown characters ***")
        except ValueError as value_error:
            logging.error("*** %s in filenames_from_date ***",
                          sys.exc_info()[0])
            logging.error(value_error)
            sys.exit("*** Value Error found while expanding XML date format!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_date ***",
                          sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML date format!")
        
        return all_dates
    
    @staticmethod
    def filenames_from_template(folder_template, template_fills):
        """! given a folder template and the values to fill in, generates
        list of filenames
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
            # generate a list of directories with all combinations of values
            # filled in
            load_dirs = [folder_template]
            for key in template_fills:
                alist = []
                for fvalue in template_fills[key]:
                    for tvalue in load_dirs:
                        alist.append(tvalue.replace("{" + key + "}", fvalue))
                load_dirs = alist
            # find all files in directories, append path to them, and put on
            # load_files list
            file_list = []
            for file_dir in load_dirs:
                if os.path.exists(file_dir):
                    for file_name in os.listdir(file_dir):
                        file_list.append(file_dir + "/" + file_name)
            # this removes duplicate file names. do we want that?
            file_list = list(dict.fromkeys(file_list))
            # remove directory names
            file_list = [lf for lf in file_list if '.' in lf.split('/')[-1]]
            logging.debug("Initial number of files: %s", str(len(file_list)))
        except ValueError as value_error:
            logging.error("*** %s in filenames_from_template ***",
                          sys.exc_info()[0])
            logging.error(value_error)
            sys.exit(
                "*** Value Error found while expanding XML folder templates!")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in filenames_from_template ***",
                          sys.exc_info()[0])
            sys.exit("*** Error found while expanding XML folder templates!")
        
        return file_list
