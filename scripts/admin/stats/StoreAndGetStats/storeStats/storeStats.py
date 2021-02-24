#!/usr/bin/env python
import json
import os
import pickle
import csv
import urllib
import requests
from bucket import Bucket
from datetime import datetime,timedelta
from cbcluster import CBCluster

cluster_list = []
ip_read_map = []
ip_store_map = []
nodes = []
buckets = []
cur_date = ""

def readClusterList():
	for item in config["clusters"]:
		cluster_list.insert(0, item)
            
def readIPList(clusterName):
	for item in cluster_list:
		if clusterName == item["name"]:
			for node in item["nodes"]:
				ip_read_map.insert(0, node)

def readIPStoreList():
	for node in config["store"]["nodes"]:
		ip_store_map.insert(0, node)

def getHost(ip_map, clusterName, useTls):
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

def getBucketStats(host, username, password, cbstore, clusterName, useTls):
	protocol = "http://"
	port = "8091"
	if useTls:
		protocol = "https://"
		port = "18091"
	url = protocol + username + ":" + password + "@" + host + ":" + port + "/pools/default/buckets"
	response = urllib.urlopen(url)
	bucket_dict = json.loads(response.read())
	bucket_id = clusterName + ":buckets:" + cur_date
	cbstore.upsert(bucket_id, bucket_dict)
	for bucket_info in bucket_dict:
		buckets.insert(0, Bucket(bucket_info))
	for bucket in buckets:
		stats_url = protocol + username + ":" + password + "@" + host + ":" + port + bucket.stats_uri
		response = urllib.urlopen(stats_url)
		bucket_stats = json.loads(response.read())
		bucket_stats_id = clusterName + ":bucket:" + bucket.name + ":stats:" + cur_date
		cbstore.upsert(bucket_stats_id, bucket_stats)
		stats_url = protocol + username + ":" + password + "@" + host + ":" + port + "/pools/default/buckets/@xdcr-" + bucket.name + "/stats"
		response = urllib.urlopen(stats_url)
		bucket_stats = json.loads(response.read())
		bucket_stats_id = clusterName + ":bucket:" + bucket.name + ":xdcr:" + cur_date
		cbstore.upsert(bucket_stats_id, bucket_stats)

def getNodeInfo(host, username, password, cbstore, clusterName, useTls):
	protocol = "http://"
	port = "8091"
	adminPort = "8093"
	if useTls:
		protocol = "https://"
		port = "18091"
		adminPort = "18093"
	url = protocol + username + ":" + password + "@" + host + ":" + port + "/pools/default"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":node:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)
	for node in response_dict["nodes"]:
		nodes.insert(0, node)
	url = protocol + username + ":" + password + "@" + host + ":" + adminPort + "/admin/stats"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":admin:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)
	url = protocol + username + ":" + password + "@" + host + ":" + port + "/pools/default/tasks"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":tasks:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)

def getQueryInfo(host, username, password, cbstore, clusterName, useTls):
	protocol = "http://"
	port = "8091"
	hostIp = host
	nodeFound = "false"
	if useTls:
		protocol = "https://"
		port = "18091"
	for node in nodes:
		for service in node["services"]:
			if unicode(service) == "n1ql":
				hostIp = node["hostname"]
				nodeFound = "true"
	url = protocol + username + ":" + password + "@" + hostIp + ":" + port + "/pools/default/buckets/@query/stats"
	if nodeFound == "true":
		url = protocol + username + ":" + password + "@" + hostIp + "/pools/default/buckets/@query/stats"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":query:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)
	
def getIndexInfo(host, username, password, cbstore, clusterName, useTls):
	protocol = "http://"
	port = "8091"
	hostIp = host
	nodeFound = "false"
	if useTls:
		protocol = "https://"
		port = "18091"
	for node in nodes:
		for service in node["services"]:
			if unicode(service) == "index":
				hostIp = node["hostname"].split(":", 1)[0]
				nodeFound = "true"
	url = protocol + username + ":" + password + "@" + hostIp + ":" + port + "/indexStatus"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":indexes:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)
	url = protocol + username + ":" + password + "@" + hostIp + ":" + port + "/pools/default/buckets/@index/stats"
	response = urllib.urlopen(url)
	response_dict = json.loads(response.read())
	node_stats_id = clusterName + ":indexStats:" + cur_date
	cbstore.upsert(node_stats_id, response_dict)
	
def getFtsInfo(host, username, password, cbstore, clusterName, useTls):
	protocol = "http://"
	port = "8094"
	hostIp = host
	nodeFound = "false"
	if useTls:
		protocol = "https://"
		port = "18094"
	for node in nodes:
		for service in node["services"]:
			if unicode(service) == "fts":
				hostIp = node["hostname"].split(":", 1)[0]
				nodeFound = "true"
	if nodeFound == "true":
		url = protocol + username + ":" + password + "@" + hostIp + ":" + port + "/api/index"
		response = urllib.urlopen(url)
		response_dict = json.loads(response.read())
		node_stats_id = clusterName + ":ftsIndexes:" + cur_date
		cbstore.upsert(node_stats_id, response_dict)
		url = protocol + username + ":" + password + "@" + hostIp + ":" + port + "/api/stats"
		response = urllib.urlopen(url)
		response_dict = json.loads(response.read())
		node_stats_id = clusterName + ":ftsStats:" + cur_date
		cbstore.upsert(node_stats_id, response_dict)
		
	

cur_datetime = datetime.now()
cur_date = datetime.now().strftime('%Y%m%d%H%M%S')
with open('config.json') as json_file:
	config = json.load(json_file)

readClusterList()
readIPStoreList()
valid_store_host = getHost(ip_store_map, "", config["store"]["tls"])
    
cb_bucket = CBCluster(config["store"], valid_store_host)
active_monitor_id = "monitor_status"
rv = cb_bucket.get(active_monitor_id)
#print "node is " + str(config["node"])
if rv != "":
	#print "last monitor node is " + str(rv.value["node"])
	#print "last monitor time is " + str(rv.value["time"])
	if str(rv.value["node"]) == str(config["node"]):
		good_to_go = "true"
	else:
		last_datetime = datetime.strptime(str(rv.value["time"]), '%Y%m%d%H%M%S')
		checkpoint_datetime = cur_datetime - timedelta(minutes=config["frequency"])
		if last_datetime < checkpoint_datetime:
			#print "Last monitor is too old, need to switch"
			good_to_go = "true"
		else:
			good_to_go = "false"
else:
	good_to_go = "true"

if good_to_go == "true":
	cb_bucket.upsert(active_monitor_id, {'node':str(config["node"]), 'time':cur_date })
	
	for cur_cluster in cluster_list:
		ip_read_map = []
		readIPList(cur_cluster["name"])
		valid_host = getHost(ip_read_map, str(cur_cluster["name"]), cur_cluster["tls"])
		if valid_host != "":
			cur_user = str(cur_cluster["user"])
			cur_pwd = str(cur_cluster["pwd"])
			nodes = []
			getNodeInfo(str(valid_host),cur_user,cur_pwd,cb_bucket, str(cur_cluster["name"]), cur_cluster["tls"])
			buckets = []
			getBucketStats(str(valid_host),cur_user,cur_pwd,cb_bucket, str(cur_cluster["name"]), cur_cluster["tls"])
			getQueryInfo(str(valid_host),cur_user,cur_pwd,cb_bucket, str(cur_cluster["name"]), cur_cluster["tls"])
			getIndexInfo(str(valid_host),cur_user,cur_pwd,cb_bucket, str(cur_cluster["name"]), cur_cluster["tls"])
			getFtsInfo(str(valid_host),cur_user,cur_pwd,cb_bucket, str(cur_cluster["name"]), cur_cluster["tls"])
