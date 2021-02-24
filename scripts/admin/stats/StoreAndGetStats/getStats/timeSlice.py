#!/usr/bin/env python
class TimeSlice(object):
	
	def __init__(self, id, timeslice):
		self.id = id
		self.timeslice = timeslice
		self.node_stats = []
		
	def addNodeStat(self, nodeStat):
		if nodeStat.timeslice == self.timeslice:
			self.node_stats.append(nodeStat)
	
	def addBucketStat(self, bucketStat):
		self.bucket_stat = bucketStat
		