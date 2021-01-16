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
from pathlib import Path
import logging
from lxml import etree
from vsdb_files_to_cb import constants as cn
import pandas as pd


class LoadXmlSpecFile:
    """! Class to read in load_spec file
        file should be an xml file
        Returns:
           N/A
    """
    
    def __init__(self, args):
        # set the defaults
        # args requires {'spec_file':something, ['spec_type':'gsd_builder'] }
        self.spec_file_name = args['spec_file']
        self.root = None
        self.host_and_port = None
        self.tree = None
        self.folder_template = None
        self.template_fills = {}
        self.date_list = {}
        
        # define and initialize a met style load_spec
        self.connection_list = ['connection']
        self.load_spec = {
            'connection': {'host': None, 'port': 3306, 'database': None,
                           'user': None, 'password': None,
                           'management_system': "mysql"},
            'flags': {'line_type_load': False, 'load_stat': True,
                      'load_mode': True, 'load_mtd': True, 'load_mpr': False,
                      'load_orank': False, 'force_dup_file': False,
                      'verbose': False, 'stat_header_db_check': True,
                      'mode_header_db_check': True,
                      'mtd_header_db_check': True, 'drop_indexes': False,
                      'apply_indexes': False, 'load_xml': True},
            'db_driver': None, 'insert_size': 1, 'load_note': None,
            'group': cn.DEFAULT_DATABASE_GROUP, 'description': None,
            'xml_str': None, 'email': None, 'initialize_db': None,
            'organization': None, 'load_files': [], 'line_types': [],
            'folder_tmpl': None}
    
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
                                                                  "be "
                                                                  "found!")
                # parse the XML file  (this is largely copied directly from
                # met_db_load)
            parser = etree.XMLParser(remove_comments=True)
            self.tree = etree.parse(self.spec_file_name, parser=parser)
            self.root = self.tree.getroot()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")
        
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
                    self.load_spec['connection']['host'] = self.host_and_port[
                        0]
                    if len(self.host_and_port) > 1:
                        self.load_spec['connection']['port'] = int(
                            self.host_and_port[1])
                    if (not self.load_spec['connection']['host']) or (
                            not self.load_spec['connection']['database']):
                        logging.warning("!!! load_spec must include host and "
                                        "database tags")
                    if (not self.load_spec['connection']['user']) or (
                            not self.load_spec['connection']['password']):
                        logging.warning("!!! load_spec must include user and "
                                        "password tags")
                    if not self.load_spec['connection']['database'].startswith(
                            "mv_"):
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
                        self.load_spec['flags']['stat_header_db_check'] = False
                elif child.tag.lower() == "mode_header_db_check":
                    if child.text.lower() == cn.LC_FALSE:
                        self.load_spec['flags']['mode_header_db_check'] = False
                elif child.tag.lower() == "mtd_header_db_check":
                    if child.text.lower() == cn.LC_FALSE:
                        self.load_spec['flags']['mtd_header_db_check'] = False
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
            
            self.load_spec['load_files'] = filenames_from_template(
                self.folder_template, self.template_fills)
        
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read xml ***", sys.exc_info()[0])
            sys.exit("*** Error(s) found while reading XML file!")
        return self.load_spec


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
        sys.exit("*** Value Error found while expanding XML folder templates!")
    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s in filenames_from_template ***",
                      sys.exc_info()[0])
        sys.exit("*** Error found while expanding XML folder templates!")
    
    return file_list


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
            date_start = pd.to_datetime(date_list["start"], format=date_format)
            date_end = pd.to_datetime(date_list["end"], format=date_format)
            date_inc = int(date_list["inc"])
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
