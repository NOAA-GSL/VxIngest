#!/usr/bin/env python
import requests
import argparse
import zlib
import json
import os
import pickle
import csv
import urllib
import requests

cluster_nodes = []
vbuckets = []

def getBucketStats(host, username, password):
    url = "http://" + username + ":" + password + "@" + host + ":8091/pools/default/buckets"
    response = urllib.urlopen(url)
    bucket_dict = json.loads(response.read())
    for bucket_info in bucket_dict:
    	if bucket_info["name"] == args.bucket:
    		#print bucket_info["name"]
    		nodes = bucket_info["nodes"]
    		vBucketServerMap = bucket_info["vBucketServerMap"]
    		vBucketMap = vBucketServerMap["vBucketMap"]
    		for node in nodes:
    			#print node["hostname"]
    			cluster_nodes.append(node["hostname"])
    		for vbucket in vBucketMap:
					vbuckets.append(vbucket)
				
parser = argparse.ArgumentParser(description='Determine Couchbase VBucket for a document key.')
parser.add_argument('--k', action='store', nargs='?', required=True, metavar='key', dest='key') 
parser.add_argument('--b', action='store', nargs='?', required=True, metavar='bucket', dest='bucket') 
parser.add_argument('--n', action='store', nargs='?', required=True, metavar='node', dest='node') 
parser.add_argument('--a', action='store', nargs='?', required=True, metavar='adminUserId', dest='admin') 
parser.add_argument('--p', action='store', nargs='?', required=True, metavar='password', dest='pwd') 

args = parser.parse_args()
getBucketStats(args.node, args.admin, args.pwd)
#print len(vbuckets)
vbucket = zlib.crc32(args.key)%len(vbuckets)
#print vbucket
nodeMap = vbuckets[vbucket]
#print nodeMap
nodeNumber = nodeMap[0]
print len(nodeMap)
#print nodeNumber
nodeName = cluster_nodes[nodeNumber]
print "Active vbucket on " + nodeName
idx = 1
while idx < len(nodeMap):
	nodeNumber = nodeMap[idx]
	nodeName = cluster_nodes[nodeNumber]
	print "Replica vbucket on " + nodeName
	idx += 1
