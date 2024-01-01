#!/usr/bin/env python3
"""
This script reads in RTO logs from the Java SDK,
correlates the orphan entries with the corresponding
threshold entries and writes out the combined log
entry data into various .CSV files. The purpose
of this script is for use in diagnosing timeout
issues to determine where the time is being taken,
client, network, or server.

This script is written for python3 (3.7 minimum) and depends
on the dictfier library. Install this library
by calling:

pip3 install dictfier

It also uses the datetime.fromisoformat method,
which was added in python 3.7, so that's the
minimum python version for using this script.

Usage:

To get the help text:
> python3 orphans_threshold.py -h

To run on a log file:
> python3 orphans_threshold.py -f <logfile> -s <startdatetime> -e <enddatetime>

Required parameters:
	-f <logfile> - This is the RTO logfile being analyzed
	-s <startdate> - This is the starting datetime point in the log
									to be analyzed. This should be in ISO format.
	-e <enddatetime> - This is the ending datetime point in the log
	                to be analyzed. This should be in ISO format.

Optional parameters:
	-o <orphans.csv> - This is the output .CSV file that the combined
									orphan and threshold log entries will be written.
	-m <mismatch.csv> - This is the output .CSV file that the combined
									orphan and threshold log entries will be written.
									The entries in this file are where the orphan and
									threshold log entries have different "server_us"
									values.
	-t <non-orphan-thresholds.csv> - This is the output .CSV file
									that will contain the threshold log entries that
									do not have a corresponding orphan log entry.
									These log entries should have made it back to the
									client SDK, but didn't complete the decoding prior
									to the timeout being reached.
	-n <nomatch.log> - This is the log file that the orphan log entries
									that have no corresponding threshold log entry found
									will be written into.
	-T <30000> - This is the threshold value that will be compared to
									for setting flags in the .CSV files indicating which
									of the three areas were responsible for the timeout
									expiring: client, network, or server.
	-l <head-of-line.txt> - This is the text file into which a list of
									timestamps will be added, marking potential occurrances
									of the "Head-of-Line" issue, where a slow server
									operation blocks subsequent request results from 
									being returned to the client promptly.

Both the start and end datetime should be in ISO format.
If including a space between the date and time, enclose the
datetime within quotes.
"""
import argparse
import csv
import json
from datetime import datetime

# pylint: disable=import-error
import dictfier

# pylint: enable=import-error

# pylint: disable=superfluous-parens
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

orphans = []
mismatches = []
thresholds = []
timeouts = []


class Orphans:
    """
    This is the collection of orphan log entries
    that are extracted from the log file.
    """
    def __init__(self, log_line):
        log_time = log_line[:24]
        self.orphs = []
        # Find the start of the json in the log entry
        top_marker = log_line.find("[{\"top\":")
        if top_marker >= 0:
            # Parse out the json portion and load it into
            # a dict object
            json_data = log_line[top_marker:]
            data_dict = json.loads(json_data)
            # Iterate through the top level objects
            for top in data_dict:
                # Iterate through the orphan entries
                for current in top["top"]:
                    # Instantiate the log entry as an Orphan object
                    # and add it to the list of entries in the
                    # log line.
                    self.orphs.append(Orphan(current, log_time))


# pylint: disable=too-many-instance-attributes
class Orphan:
    """
    This is a single orphan log entry from
    the RTO log file.
    """
    def __init__(self, data_dict, log_time):
        self.log_time = log_time
        self.raw_json = data_dict
        self.bucket = data_dict["b"]
        self.last_local_address = data_dict["l"]
        self.last_local_id = data_dict["c"]
        self.last_operation_id = data_dict["i"]
        self.last_remote_address = data_dict["r"]
        self.server_us = data_dict["d"]
# pylint: enable=too-many-instance-attributes


class Thresholds:
    """
    This is the collection of threshold log entries
    that are extracted from the log file.
    """
    def __init__(self, log_line, latency_threshold):
        log_time = log_line[:24]
        self.threshs = []
        # Find the start of the json in the log entry
        top_marker = log_line.find("[{\"top\":")
        if top_marker >= 0:
            # Parse out the json portion and load it into
            # a dict object
            json_data = log_line[top_marker:]
            data_dict = json.loads(json_data)
            # Iterate through the top level objects
            for top in data_dict:
                # Iterate through the threshold entries
                for current in top["top"]:
                    # Instantiate the log entry as an Threshold object
                    # and add it to the list of entries in the
                    # log line.
                    self.threshs.append(Threshold(current, log_time, latency_threshold))


# pylint: disable=too-many-instance-attributes
class Threshold:
    """
    This is a single threshold log entry from
    the RTO log file.
    """
    def __init__(self, data_dict, log_time, latency_threshold):
        self.log_time = log_time
        self.operation = data_dict["operation_name"]
        self.server_us = 0
        self.encode_us = 0
        self.decode_us = 0
        if "server_us" in data_dict:
            self.server_us = data_dict["server_us"]
        if "encode_us" in data_dict:
            self.encode_us = data_dict["encode_us"]
        if "decode_us" in data_dict:
            self.decode_us = data_dict["decode_us"]
        self.last_local_id = data_dict["last_local_id"]
        self.last_local_address = data_dict["last_local_address"]
        self.last_remote_address = data_dict["last_remote_address"]
        self.last_dispatch_us = data_dict["last_dispatch_us"]
        self.last_operation_id = data_dict["last_operation_id"]
        self.total_us = data_dict["total_us"]
        self.network_time = self.last_dispatch_us - self.server_us
        self.scheduling_time = self.total_us - self.decode_us - self.encode_us - \
        				self.last_dispatch_us
        self.client_time = self.scheduling_time + self.decode_us + self.encode_us
        self.slow_client = 1 if self.client_time > latency_threshold else 0
        self.slow_network = 1 if self.network_time > latency_threshold else 0
        self.slow_server = 1 if self.server_us > latency_threshold else 0
        slow_components = self.slow_client + self.slow_network + self.slow_server
        self.slow_client_only = 1 if self.slow_client == 1 and slow_components == 1 else 0
        self.slow_network_only = 1 if self.slow_network == 1 and slow_components == 1 else 0
        self.slow_server_only = 1 if self.slow_server == 1 and slow_components == 1 else 0
        self.slow_client_network = 1 if self.slow_client == 1 and self.slow_network == 1 and \
        				slow_components == 2 else 0
        self.slow_client_server = 1 if self.slow_client == 1 and self.slow_server and \
        				slow_components == 2 else 0
        self.slow_network_server = 1 if self.slow_network == 1 and self.slow_server == 1 and \
        				slow_components == 2 else 0
        self.slow_all = 1 if slow_components == 3 else 0



class Timeout:
    """
    This is the combined orphan and threshold log
    entries that will be written out to the .CSV file.
    """
    def __init__(self, orphan, threshold):
        self.log_time = orphan.log_time
        self.bucket = orphan.bucket
        self.operation = threshold.operation
        self.server_us = threshold.server_us
        self.encode_us = threshold.encode_us
        self.decode_us = threshold.decode_us
        self.total_us = threshold.total_us
        self.network_time = threshold.network_time
        self.scheduling_time = threshold.scheduling_time
        self.last_dispatch_us = threshold.last_dispatch_us
        self.last_local_id = threshold.last_local_id
        self.last_local_address = threshold.last_local_address
        self.last_remote_address = threshold.last_remote_address
        self.client_time = threshold.client_time
        self.slow_client = threshold.slow_client
        self.slow_network = threshold.slow_network
        self.slow_server = threshold.slow_server
        self.slow_client_only = threshold.slow_client_only
        self.slow_network_only = threshold.slow_network_only
        self.slow_server_only = threshold.slow_server_only
        self.slow_client_network = threshold.slow_client_network
        self.slow_client_server = threshold.slow_client_server
        self.slow_network_server = threshold.slow_network_server
        self.slow_all = threshold.slow_all
# pylint: enable=too-many-instance-attributes



class Mismatch(Timeout):
    """
    This is the combined orphan and threshold log
    entries where the "server_us" time does not
    match.
    """
    def __init__(self, orphan, threshold):
        Timeout.__init__(self, orphan, threshold)
        self.orphan_server_us = orphan.server_us



# pylint: disable=no-member
def main():
    """
    This is the main processing method.
    """
    # Parse out the command line arguments
    args = parse_args()
    # Read in the log file, extracting the orphan and threshold entries
    read_log_file(args.log, datetime.fromisoformat(args.startdate), \
    				datetime.fromisoformat(args.enddate), args.threshold)
    # Correlate the orphan and threshold log entries
    correlate_orphans_and_thresholds(args.nomatch)
    # Write out the orphans .csv file
    write_orphans_file(args.orphans)
    # Write out the mismatch .csv file
    write_mismatch_file(args.mismatch)
    # Write out the remaining threshold entries .csv file
    identify_non_orphans(args.non_orphans)
    # Scan for potential Head-Of-Line occurrances
    scan_for_hol(args.head_of_line)
# pylint: enable=no-member


def parse_args():
    """
    This method parses out the command-line parameters.
    """
    parser = argparse.ArgumentParser(
        description='Grab all stats for a cluster and save in a file.')
    parser.add_argument('-f', action='store', nargs='?', required=True, metavar='sdk-rto.log',
                        dest='log')
    parser.add_argument('-s', action='store', nargs='?', required=True,
                        metavar='<starting datetime>', dest='startdate')
    parser.add_argument('-e', action='store', nargs='?', required=True,
                        metavar='<ending datetime>', dest='enddate')
    parser.add_argument('-o', action='store', nargs='?', required=False, metavar='orphans.csv',
                        dest='orphans', default='orphans.csv')
    parser.add_argument('-t', action='store', nargs='?', required=False,
                        metavar='non-orphan-thresholds.csv', dest='non_orphans',
                        default='non-orphan-thresholds.csv')
    parser.add_argument('-T', action='store', nargs='?', required=False, metavar='30000',
                        dest='threshold', default=30000)
    parser.add_argument('-m', action='store', nargs='?', required=False, metavar='mismatch.csv',
                        dest='mismatch', default='mismatch.csv')
    parser.add_argument('-n', action='store', nargs='?', required=False, metavar='nomatch.log',
                        dest='nomatch', default='nomatch.log')
    parser.add_argument('-l', action='store', nargs='?', required=False, metavar='head-of-line.txt',
                        dest='head_of_line', default='head-of-line.txt')
    return parser.parse_args()


# pylint: disable=no-member
def read_log_file(log_file, start_date, end_date, threshold):
    """
    This method reads in the RTO log file and extracts the orphan and
    threshold entries.
    """
    # Open and read in the file
    input_file = open(log_file)
    # Iterate through the lines in the log file
    for line in input_file:
        # Extract the datetime from the line and compare it to
        # the start and end datetimes.
        if line[:2] == "20":
            line_date = datetime.fromisoformat(line[:19])
            if start_date <= line_date <= end_date:
                # Determine if this line is either an orphan or
                # threshold entry.
                orphan_marker = line.find("cb-orphan-1")
                tracing_marker = line.find("cb-tracing-1")
                # If an orphan, extract the entries
                # and add them to the orphans list
                if orphan_marker > 0:
                    orphs = Orphans(line)
                    for cur_orph in orphs.orphs:
                        orphans.append(cur_orph)
                # If an threshold, extract the entries
                # and add them to the thresholds list
                if tracing_marker > 0:
                    threshs = Thresholds(line, threshold)
                    for cur_thresh in threshs.threshs:
                        thresholds.append(cur_thresh)
    # Close the log file
    input_file.close()
# pylint: enable=no-member


def correlate_orphans_and_thresholds(nomatch_file):
    """
    This method correlates the orphans and threshold log entries. It does this
    by iterating through the list of orphans, and searching for the matching
    last_operation_id in the threshold list. If found, and the server times
    match, they are combined and added to the timeouts list. The found and
    the server times do not match, they are combined and added to the mismatch
    list. If no corresponding threshold is found, the orphan is written out
    to the nomatch file.
    """
    # Open the nomatch file
    out_file = open(nomatch_file, "w")
    nomatch_count = 0
    # Iterate through the orphans
    for cur_orphan in orphans:
        # Set a flag to know if we found a match
        no_match = True
        # Iterate through the thresholds
        for cur_thresh in thresholds:
            # If the operation id and server times match, add it to
            # the timeout list
            if (cur_thresh.last_operation_id == cur_orphan.last_operation_id) and (
                    cur_orphan.server_us == cur_thresh.server_us):
                timeouts.append(Timeout(cur_orphan, cur_thresh))
                no_match = False
            # If the operation ids match, but the server times don't
            # add it to the mismatch list
            elif (cur_thresh.last_operation_id == cur_orphan.last_operation_id) and (
                    cur_orphan.server_us != cur_thresh.server_us):
                mismatches.append(Mismatch(cur_orphan, cur_thresh))
                no_match = False
        # Did we find a matching threshold?
        if no_match:
            # If not, write it out to the nomatch file
            nomatch_count += 1
            out_file.write(cur_orphan.log_time + " " + str(cur_orphan.raw_json) + "\n")
    # Close the nomatch file and let the user know how many don't have a match
    out_file.close()
    print("No-match count (correlating orphans to thresholds) = " + str(nomatch_count))


def write_csv(file_name, rows, fieldnames):
    """
    This is a generic method for writing out the .csv files.
    """
    # Open the .csv file for writing
    with open(file_name, mode='w') as out_file:
        # Write out the headings line
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        # Iterate through the rows, writing out the data
        for cur in rows:
            writer.writerow(dictfier.dictfy(cur, fieldnames))


def write_orphans_file(orphans_file):
    """
    This method writes out the orphans.csv file.
    """
    # Define the field names
    fieldnames = ['log_time', 'bucket', 'operation', 'server_us', 'encode_us', 'decode_us',
                  'total_us', 'network_time', 'scheduling_time', 'last_dispatch_us',
                  'last_local_id', 'last_local_address', 'last_remote_address', 'client_time',
                  'slow_client', 'slow_network', 'slow_server', 'slow_client_only',
                  'slow_network_only', 'slow_server_only', 'slow_client_network',
                  'slow_client_server', 'slow_network_server', 'slow_all']
    # Write out the csv file
    write_csv(orphans_file, timeouts, fieldnames)


def write_mismatch_file(mismatch_file):
    """
    This method writes out the mismatch.csv file.
    """
    # Define the field names
    fieldnames = ['log_time', 'bucket', 'operation', 'server_us', 'orphan_server_us', 'encode_us',
                  'decode_us', 'total_us', 'network_time', 'scheduling_time', 'last_dispatch_us',
                  'last_local_id', 'last_local_address', 'last_remote_address', 'client_time',
                  'slow_client', 'slow_network', 'slow_server', 'slow_client_only',
                  'slow_network_only', 'slow_server_only', 'slow_client_network',
                  'slow_client_server', 'slow_network_server', 'slow_all']
    # Write out the csv file
    write_csv(mismatch_file, mismatches, fieldnames)


def identify_non_orphans(non_orphans_file):
    """
    This method identifies the threshold entries that don't have a corresponding
    orphan log entry. Once identified, it writes them out into their own csv file.
    """
    thresholdtimeouts = []
    nonmatch = 0
    # Iterate through the threshold entries
    for cur_thresh in thresholds:
        no_match = True
        # Iterate through the orphan entries
        for cur_orphan in orphans:
            # Check to see if it has a matching orphan
            if (cur_thresh.last_operation_id == cur_orphan.last_operation_id):
                no_match = False
        # If no orphan was found, append it to the list of non-orphan timeouts
        if no_match:
            nonmatch += 1
            thresholdtimeouts.append(cur_thresh)
    print("Non-Orphan Threshold Timeout Count = " + str(nonmatch))
    # Define the field names
    fieldnames = ['log_time', 'operation', 'server_us', 'encode_us', 'decode_us', 'total_us',
                  'network_time', 'scheduling_time', 'last_dispatch_us', 'last_local_id',
                  'last_local_address', 'last_remote_address', 'client_time', 'slow_client',
                  'slow_network', 'slow_server', 'slow_client_only', 'slow_network_only',
                  'slow_server_only', 'slow_client_network', 'slow_client_server',
                  'slow_network_server', 'slow_all']
    # Write out the csv file
    write_csv(non_orphans_file, thresholdtimeouts, fieldnames)

def scan_for_hol(hol_file):
    """
    This method will scan the array of timeouts looking for potential 'Head-of-line'
    situations.
    """
    # Open the Head Of Line file
    with open(hol_file, mode='w') as out_file:
        idx = 0
        max_idx = len(timeouts)
        while idx < (max_idx - 1):
            # Identify slow server timeouts
            if timeouts[idx].slow_server == 1:
                # Followed by slow network timeouts
                if timeouts[(idx + 1)].slow_network == 1:
                    # Check that the time, bucket, and both addresses match
                    if ((timeouts[idx].log_time == timeouts[(idx + 1)].log_time) and
                            (timeouts[idx].bucket == timeouts[(idx + 1)].bucket) and
                            (timeouts[idx].last_local_address == timeouts[(idx + 1)]
                             .last_local_address) and
                            (timeouts[idx].last_remote_address == timeouts[(idx + 1)]
                             .last_remote_address)):
            		        # Write out that we've identified a potential Head-Of-Line situation
                        print("Potential Head-of-Line identified at " + timeouts[idx].log_time)
                        out_file.write("Potential Head-of-Line identified at " +
                                       timeouts[idx].log_time + "\n")
            idx += 1

# Processing begins here by calling the main method.
if __name__ == '__main__':
    main()
