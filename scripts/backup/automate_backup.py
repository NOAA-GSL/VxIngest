#! /usr/bin/python

# pylint: disable=C0303, C0325

__author__ = "tdenton"
__date__ = "$Sep 20, 2019$"
__email__ = "tyler.denton@couchbase.com"
__maintainer__ = "tdenton"
__version__ = "0.0.1"

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime

from config import Config


class BackerUpper:
    '''This class is what actually does the backing up, archiving and cleaning of old backups'''
    def __init__(self, hostname="", port="", username="", password=""):
        self.instance = self.get_config()
        self.get_repo_name()
        if hostname != "" and hostname != None:
            self.instance.hostname = hostname
        if port != "" and port != None:
            self.instance.port = port
        if username != "" and username != None:
            self.instance.username = username
        if password != "" and password != None:
            self.instance.password = password

    def get_config(self):
        '''This gets the config from the json config file and converts it to the class definition'''
        myfile = open("./config.json")
        myfile_str = myfile.read()
        myfile.close()
        myfile_json = json.loads(myfile_str)
        instance = Config(myfile_json)
        return instance

    def get_old_repos(self):
        '''Gets a list of the old backup archives'''
        repo_list = os.listdir(self.instance.archive)
        repo_list.remove(self.instance.repo)
        repo_list.remove("logs")
        repo_list.remove(".backup")
        for entry in repo_list:
            #entry_dt = datetime.strptime(entry, "%Y-%m-%d")
            days_old = (datetime.strptime(entry, "%Y-%m-%d") - 
                        datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")).days
            if days_old > self.instance.weeks_to_keep * 7:
                self.remove_repo(entry)

    def get_repo_name(self):
        '''Sets the name of the current repository'''
        if not self.instance.specify_repo:
            #self.instance.repo = datetime.now().strftime("%Y-%m-%d")
            self.instance.repo = "current"

    def archive_repo(self):
        '''Copys the existing current backup and turns it into an archive'''
        shutil.copytree(f"{self.instance.archive}/{self.instance.repo}", 
                        "{}/{}".format(self.instance.archive, datetime.now().strftime("%Y-%m-%d")),
                        symlinks=False,
                        ignore=None)
        self.remove_repo(self.instance.repo)
        self.create_archive()
        self.backup_repo()

    def backup_repo(self):
        '''This performs the backup'''
        backup_array = ["/opt/couchbase/bin/cbbackupmgr", 
                        "backup", 
                        "-a", 
                        self.instance.archive, 
                        "-r", 
                        self.instance.repo, 
                        "-c", 
                        f"{self.instance.hostname}:{self.instance.port}",
                        "-u",
                        self.instance.username,
                        "-p",
                        self.instance.password]
        if self.instance.value_compression != "unchanged":
            backup_array.append(f"--value_compression {self.instance.value_compression}")
        if self.instance.threads != 1:
            backup_array.append(f"--threads {self.instance.threads}")
        if self.instance.no_progress_bar:
            backup_array.append("--no-progress-bar")
        try:
            response = subprocess.check_output(backup_array)
        except Exception as e:
            response = "Error backing up: " + str(e)
        print(response)

    def remove_repo(self, repo_name):
        '''This deletes the old repo'''
        remove_array = ["/opt/couchbase/bin/cbbackupmgr", 
                        "remove",
                        "-a", 
                        self.instance.archive, 
                        "-r", 
                        repo_name]
        print(remove_array)
        try:
            response = subprocess.check_output(remove_array)
        except Exception as e:
            response = "Error removing backup: " + str(e)
        print(response)

    def create_archive(self):
        '''This creates the initial archive and and the current backup repo'''
        script_array = ["/opt/couchbase/bin/cbbackupmgr",
                        "config",
                        "-a",
                        self.instance.archive,
                        "-r",
                        self.instance.repo]
        if self.instance.exclude_buckets and not self.instance.include_buckets:
            script_array.append("--exclude-buckets")
            script_array.append(",".join(self.instance.exclude_bucket_list))
        elif not self.instance.exclude_buckets and self.instance.include_buckets:
            script_array.append("--include-buckets")
            script_array.append(",".join(self.instance.include_bucket_list))
        elif self.instance.exclude_buckets and self.instance.include_buckets:
            print("cannot both include and exclude")
            exit(1)

        if self.instance.disable_bucket_config:
            script_array.append("--disable-bucket-config")

        if self.instance.disable_views:
            script_array.append("--disable-views")

        if self.instance.disable_fts_indexes:
            script_array.append("--disable-ft-indexes")

        if self.instance.disable_fts_alias:
            version_array = self.instance.cb_version.split(".")
            if int(version_array[0]) >= 6 and int(version_array[1]) >= 5:
                script_array.append("--disable-ft-alias")

        if self.instance.disable_gsi_indexes:
            script_array.append("--disable-gsi-indexes")

        if self.instance.disable_data:
            script_array.append("--disable-data")

        if self.instance.disable_analytics:
            version_array = self.instance.cb_version.split(".")
            if int(version_array[0]) >= 6 and int(version_array[1]) >= 5:
                script_array.append("--disable-analytics")

        if self.instance.disable_eventing:
            script_array.append("--disable-eventing")

        if self.instance.vbucket_filter:
            script_array.append("--vbucket-filter")
            script_array.append(self.instance.vbucket_filter_list)

        try:
            response = subprocess.check_output(script_array)
        except:
            response = "Archive and repo already created"
        print(response)
	
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        "Please use the config.json file in this dir to configure the backup settings")
    PARSER.add_argument("function", 
                        choices=['createArchive',
                                 'backup',
                                 'archiveBackup',
                                 'cleanOld'])
    PARSER.add_argument("--username")
    PARSER.add_argument("--password")
    PARSER.add_argument("--hostname")
    PARSER.add_argument("--port")
    ARGS = PARSER.parse_args()   
 
    BACKERUPER = BackerUpper(ARGS.hostname, ARGS.port, ARGS.username, ARGS.password)
    if ARGS.function == "createArchive":
        BACKERUPER.create_archive()
    elif ARGS.function == "backup":
        BACKERUPER.backup_repo()
    elif ARGS.function == "archiveBackup":
        BACKERUPER.archive_repo()
    elif ARGS.function == "cleanOld":
        BACKERUPER.get_old_repos()
