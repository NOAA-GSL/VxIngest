#!/usr/bin/env python
import json
import os
import pickle
import csv
import urllib
import requests
import argparse
from bucket import Bucket
from node import Node
from datetime import datetime,timedelta
from cbcluster import CBCluster
from timeSlice import TimeSlice
from fileOutput import FileOutput
from nodeOutput import NodeOutput

ip_store_map = []
time_slices = []

def readIPStoreList():
	for node in config["store"]["nodes"]:
		ip_store_map.insert(0, node)

def getHost(ip_map, useTls):
	host = ""
	protocol = "http://"
	port = "8091"
	if useTls:
		protocol = "https://"
		port = "18091"
	try:
		for host_ip in ip_map:
			url = protocol + str(host_ip) + ":" + port + "/pools/default"
			r = requests.head(url)
			http_status = str(r.status_code)
			host = host_ip
			if http_status == "200" or http_status == "401":
				return host
	except Exception as ex:
		print "No Connection to any hosts in ip_list"
		print ex
	return host

def getBucketStats(bucketName, timeSlice):
	bucket_id = args.cluster + ":buckets:" + timeSlice
	rb = cb_bucket.get(bucket_id)
	if rb != '':
		for cur_bucket in rb.value:
			if cur_bucket["name"] == bucketName:
				bucket = Bucket(cur_bucket)
				docId = args.cluster + ":bucket:" + bucketName + ":stats:" + timeSlice
				rs = cb_bucket.get(docId)
				bucket.setStats(rs.value)
				return bucket
    

def getNodeStats():
	bucketName = config["store"]["bucket"]
	n1ql = "Select *, meta().id from " + bucketName + " where meta().id >= $1 and meta().id <= $2 order by meta().id asc"
	arg1 = args.cluster + ":node:" + args.startDate + "000000"
	arg2 = args.cluster + ":node:" + args.endDate + "000000"
	rs = cb_bucket.query(n1ql, arg1, arg2)
	for row in rs:
		response_dict = row[bucketName]
		response_id = row['id']
		response_datetime = response_id[-14:]
		ts = TimeSlice(response_id, response_datetime)
		node_dict = response_dict['nodes']
		for node_info in node_dict:
			ts.addNodeStat(Node(node_info, response_datetime))
		ts.addBucketStat(getBucketStats(args.bucket, response_datetime))
		time_slices.append(ts)

def writeOutputFile():
	fileOutput = FileOutput(args.fileName)
	for ts in time_slices:
		if ts.bucket_stat != None:
			fileOutput.addTimeSlice(ts)
	fileOutput.outputFile()
    
end_datetime = datetime.now() + timedelta(days=1)
end_date = end_datetime.strftime('%Y%m%d')
parser = argparse.ArgumentParser(description='Extract couchbase stats for the specified timeframe.')
parser.add_argument('--c', action='store', nargs='?', required=True, metavar='cluster', dest='cluster') 
parser.add_argument('--b', action='store', nargs='?', required=True, metavar='bucket', dest='bucket') 
parser.add_argument('--s', action='store', nargs='?', required=True, metavar='start', dest='startDate') 
parser.add_argument('--e', action='store', nargs='?', required=False, metavar='end', dest='endDate', default=end_date) 
parser.add_argument('--f', action='store', nargs='?', required=False, metavar='outputFile', dest='fileName', default='cluster_stats.csv')

args = parser.parse_args()
with open('config.json') as json_file:
	config = json.load(json_file)
readIPStoreList()
valid_store_host = getHost(ip_store_map, config["store"]["tls"])
cb_bucket = CBCluster(config["store"], valid_store_host)

getNodeStats()
writeOutputFile()