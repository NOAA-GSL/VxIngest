#!/usr/bin/python
# -*- coding: utf-8 -*-

# pylint: disable=C0303, C0325

# This script it used to analyze indexes. It should work for standard indexes
# and array indexes. Will tell you they type of index, if it is a primary index,
# the size of the docIDs in the index, the size of the values in the index,
# the number of records in the index, the selectivity of the index, and
# calculates the average number of records in te index for each docID.
# This must be run from a node in the cluster with a user who has access to run
# applications in the /opt/couchbase/bin folder.

__author__ = 'tdenton'
__date__ = '$Jul 1, 2020$'
__email__ = 'tyler.denton@couchbase.com'
__maintainer__ = 'tdenton'
__version__ = '0.0.1'

import subprocess
import argparse
import json
import urllib2
import socket
import platform
import ssl

DEFAULT_MAX_RECORDS = 10000

class Main():
    '''This is the main class that  process the indexes'''
    def __init__(self, bucket, username, password, max_records=None):
        self.bucket = bucket
        self.username = username
        self.password = password
        self.max_records = max_records
        self.current_items = 0
        self.index_list = []
        self.index_dict = {}
        self.python_version = platform.python_version()
        self.run()

    def __get_hostname(self):
        '''Because localhost doesnt work with ssl we have to get the hostname'''
        self.hostname = socket.gethostname()

    def __get_bucket_stats(self):
        '''This uses a rest call against the https port for the local node to
        get the number of documents in the bucket'''
        self.__get_hostname()
        _url = \
            'https://{}:18091/pools/default/buckets/{}'.format(self.hostname, self.bucket)
        req = urllib2.Request(_url, headers={
            'Authorization': 'Basic ' + '{}:{}'.format(self.username,
                    self.password).encode('base64').rstrip(),
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'User-Agent': 'check_version/1',
            })
        major, minor, patch = self.python_version.split(".")
        if (int(major) == 2 and int(minor) <= 7 and int(patch) < 9) or (int(major) == 3 and int(minor) <= 4):
            f = urllib2.urlopen(req, timeout=1).read()
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            f = urllib2.urlopen(req, timeout=1, context = ctx).read()
        result = json.loads(f)
        return result['nodes'][0]['interestingStats']['curr_items_tot']

    def __calc_string_length(self, values):
        '''Calculatates the number of bytes in a string'''
        try:
            return len(values.decode("utf-8").encode('utf-8'))
        except:
            print values
            return 0

    def __update_average(self, x, n, s):
        '''updates an average if you know the mean and the number of records
        that were used to calculate it'''
        return(((x*n)+s)/(n+1))

    def __records_per_id(self, index, records):
        '''This calculates the selectivity, key size, value size, records per
        docID and other values'''
        holding_dict = {}
        for entry in records.split('\n'):
            try:
                (values, key) = entry.split(' ... ')
                if key in holding_dict:
                    size = self.__calc_string_length(values)
                    holding_dict[key]['size'] = \
                        self.__update_average(holding_dict[key]['size'],
                            holding_dict[key]['count'], size)
                    holding_dict[key]['count'] += 1
                else:
                    holding_dict[key] = {'count': 1,
                            'size': self.__calc_string_length(values)}
            except Exception, e:
                pass
        key_size = 0
        value_size = 0
        key_length = 0.0
        for (_index, key) in enumerate(holding_dict):
            value_size = self.__update_average(value_size, _index,
                    holding_dict[key]['size'])
            key_size = self.__update_average(key_size, _index,
                    self.__calc_string_length(key))
            key_length = self.__update_average(float(key_length),
                    float(_index), float(holding_dict[key]['count']))
        self.index_dict[index]['doc_id_size'] = key_size
        self.index_dict[index]['value_size'] = value_size
        self.index_dict[index]['records_per_doc'] = key_length
        self.index_dict[index]['num_docs'] = \
            int(float(self.index_dict[index]['length'])
                / float(self.index_dict[index]['records_per_doc']))
        self.index_dict[index]['selectivity'] = \
            float(self.index_dict[index]['num_docs']) \
            / float(self.current_items)

    def get_indexes(self):
        '''This uses the cbindex tool in /opt/couchbase/bin to list each index,
        filter down to the indexes in the designated bucket, and determines
        the type of index.'''
        operation_array = ['/opt/couchbase/bin/cbindex', '-auth',
                           '{}:{}'.format(self.username,
                           self.password), '-type', 'list']
        response = subprocess.check_output(operation_array)
        array_response = response.split('\n')
        for response_item in array_response[1:]:
            try:
                if response_item[0] == ' ':
                    next
                else:
                    (bucket, index) = response_item.split(','
                            )[0].split(':')[1].split('/')
                    if bucket == self.bucket:
                        self.index_list.append(index)
                        self.index_dict[index] = {'primary': False,
                                'type': 'plain', 'length': 0}
                        if 'isPrimary:true' in response_item:
                            self.index_dict[index]['primary'] = True
                        if 'array' in response_item:
                            self.index_dict[index]['type'] = 'array'
            except Exception, e:
                pass

    def get_index_counts(self, index):
        '''This gets the number of records stored in the index'''
        operation_array = [
            '/opt/couchbase/bin/cbindex',
            '-auth',
            '{}:{}'.format(self.username, self.password),
            '-type',
            'count',
            '-bucket',
            self.bucket,
            '-index',
            index,
            ]
        response = subprocess.check_output(operation_array)
        list_response = response.split(' has ')
        if len(list_response) == 2:
            self.index_dict[index]['length'] = \
                list_response[1].split(' ')[0]

    def sample_index(self, index):
        '''This gets a sample of the records in the index. This could be more
        accurate with randomized sampling, currently it starts at the beginning
        of the index and grabs the first X records as defined by the max_records'''
        if self.index_dict[index]['primary'] == True:
            pass
        elif int(self.index_dict[index]['length']) == 0:
            pass
        else:
            operation_array = [
                '/opt/couchbase/bin/cbindex',
                '-auth',
                '{}:{}'.format(self.username, self.password),
                '-type',
                'scanAll',
                '-bucket',
                self.bucket,
                '-index',
                index,
                ]
            if self.max_records is not None:
                operation_array.extend(['-limit',
                        str(self.max_records)])
            else:
                if int(self.index_dict[index]['length']) < DEFAULT_MAX_RECORDS:
                    operation_array.extend(['-limit',
                            str(self.index_dict[index]['length'])])
                else:
                    operation_array.extend(['-limit', '10000'])
            response = subprocess.check_output(operation_array)
            self.__records_per_id(index, response)

    def run(self):
        '''This is the control for the object. First it gets current items in
        the bucket. Then it gets the list of index names. Then it iterates through
        the list to get the stats for each index. '''
        self.current_items = self.__get_bucket_stats()
        self.get_indexes()
        for index in self.index_list:
            self.get_index_counts(index)
            self.sample_index(index)
        print json.dumps(self.index_dict, indent=4)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser('Generate stats from indexes')
    PARSER.add_argument('--username', required=True)
    PARSER.add_argument('--password', required=True)
    PARSER.add_argument('--bucket', required=True)
    PARSER.add_argument('--max_records', default=None)

    args = PARSER.parse_args()
    main = Main(args.bucket, args.username, args.password,
                args.max_records)
