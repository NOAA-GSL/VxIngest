#!/usr/bin/env python
from nodeOutput import NodeOutput

class FileOutput(object):
	header_line = "\"Date Time\""
	quota_used = "\"quotaPercentUsed\""
	itemCount = "\"itemCount\""
	memUsed = "\"memUsed\""
	ep_bg_fetched = "\"ep_bg_fetched\""
	vb_active_resident_items_ratio = "\"vb_active_resident_items_ratio\""
	vb_replica_resident_items_ratio = "\"vb_replica_resident_items_ratio\""
	ep_tmp_oom_errors = "\"ep_tmp_oom_errors\""
	ep_oom_errors = "\"ep_oom_errors\""
	ep_queue_size = "\"ep_queue_size\""
	ep_flusher_todo = "\"ep_flusher_todo\""
	ep_dcp_views_items_remaining = "\"ep_dcp_views_items_remaining\""
	ep_dcp_2i_items_remaining = "\"ep_dcp_2i_items_remaining\""
	vb_replica_num = "\"vb_replica_num\""
	vb_active_num  = "\"vb_active_num\""
	ep_mem_low_wat = "\"ep_mem_low_wat\""
	ep_mem_high_wat = "\"ep_mem_high_wat\""
	ep_item_commit_failed = "\"ep_item_commit_failed\""
	ep_cache_miss_rate = "\"ep_cache_miss_rate\""
	couch_docs_fragmentation = "\"couch_docs_fragmentation\""
	ep_diskqueue_fill = "\"ep_diskqueue_fill\""
	ep_diskqueue_drain = "\"ep_diskqueue_drain\""
	evictions = "\"evictions\""
	memUsedPercentOfLowWatMark = "\"memUsedPercentOfLowWatMark\""
	memUsedPercentOfHighWatMark = "\"memUsedPercentOfHighWatMark\""
	ep_diskqueue_fill_drain_ratio = "\"ep_diskqueue_fill_drain_ratio\""
	nodes = []
	
	
	def __init__(self, fileName):
		self.fileName = fileName

	def addTimeSlice(self, timeSlice):
		self.header_line += ",\"" + timeSlice.timeslice + "\""
		self.quota_used += "," + str(timeSlice.bucket_stat.quotaPercentUsed)
		self.itemCount += "," + str(timeSlice.bucket_stat.itemCount)
		self.memUsed += "," + str(timeSlice.bucket_stat.memUsed)
		self.ep_bg_fetched += "," + str(timeSlice.bucket_stat.attribute_map["ep_bg_fetched"])
		self.vb_active_resident_items_ratio += "," + str(timeSlice.bucket_stat.attribute_map["vb_active_resident_items_ratio"])
		self.vb_replica_resident_items_ratio += "," + str(timeSlice.bucket_stat.attribute_map["vb_replica_resident_items_ratio"])
		self.ep_tmp_oom_errors += "," + str(timeSlice.bucket_stat.attribute_map["ep_tmp_oom_errors"])
		self.ep_oom_errors += "," + str(timeSlice.bucket_stat.attribute_map["ep_oom_errors"])
		self.ep_queue_size += "," + str(timeSlice.bucket_stat.attribute_map["ep_queue_size"])
		self.ep_flusher_todo += "," + str(timeSlice.bucket_stat.attribute_map["ep_flusher_todo"])
		self.ep_dcp_views_items_remaining += "," + str(timeSlice.bucket_stat.attribute_map["ep_dcp_views_items_remaining"])
		self.ep_dcp_2i_items_remaining += "," + str(timeSlice.bucket_stat.attribute_map["ep_dcp_2i_items_remaining"])
		self.vb_replica_num += "," + str(timeSlice.bucket_stat.attribute_map["vb_replica_num"])
		self.vb_active_num += "," + str(timeSlice.bucket_stat.attribute_map["vb_active_num"])
		self.ep_mem_low_wat += "," + str(timeSlice.bucket_stat.attribute_map["ep_mem_low_wat"])
		self.ep_mem_high_wat += "," + str(timeSlice.bucket_stat.attribute_map["ep_mem_high_wat"])
		self.ep_item_commit_failed += "," + str(timeSlice.bucket_stat.attribute_map["ep_item_commit_failed"])
		self.ep_cache_miss_rate += "," + str(timeSlice.bucket_stat.attribute_map["ep_cache_miss_rate"])
		self.couch_docs_fragmentation += "," + str(timeSlice.bucket_stat.attribute_map["couch_docs_fragmentation"])
		self.ep_diskqueue_fill += "," + str(timeSlice.bucket_stat.attribute_map["ep_diskqueue_fill"])
		self.ep_diskqueue_drain += "," + str(timeSlice.bucket_stat.attribute_map["ep_diskqueue_drain"])
		self.evictions += "," + str(timeSlice.bucket_stat.attribute_map["evictions"])
		self.memUsedPercentOfLowWatMark += "," + str(timeSlice.bucket_stat.attribute_map["memUsedPercentOfLowWatMark"])
		self.memUsedPercentOfHighWatMark += "," + str(timeSlice.bucket_stat.attribute_map["memUsedPercentOfHighWatMark"])
		self.ep_diskqueue_fill_drain_ratio += "," + str(timeSlice.bucket_stat.attribute_map["ep_diskqueue_fill_drain_ratio"])
		for nodeStat in timeSlice.node_stats:
			#print nodeStat.hostname + " - " + nodeStat.timeslice
			nodeExists = False
			for curNode in self.nodes:
				if curNode.nodeName == nodeStat.hostname:
					nodeExists = True
					outNode = curNode
			if nodeExists == False:
				outNode = NodeOutput(nodeStat.hostname)
				self.nodes.append(outNode)
			outNode.addTimeSlice(nodeStat, timeSlice.timeslice)

	def outputFile(self):
		f = open(self.fileName, "w")
		f.write(self.header_line + "\n")
		f.write(self.quota_used + "\n")
		f.write(self.itemCount + "\n")
		f.write(self.memUsed + "\n")
		f.write(self.ep_bg_fetched + "\n")
		f.write(self.vb_active_resident_items_ratio + "\n")
		f.write(self.vb_replica_resident_items_ratio + "\n")
		f.write(self.ep_tmp_oom_errors + "\n")
		f.write(self.ep_oom_errors + "\n")
		f.write(self.ep_queue_size + "\n")
		f.write(self.ep_flusher_todo + "\n")
		f.write(self.ep_dcp_views_items_remaining + "\n")
		f.write(self.ep_dcp_2i_items_remaining + "\n")
		f.write(self.vb_replica_num + "\n")
		f.write(self.vb_active_num + "\n")
		f.write(self.ep_mem_low_wat + "\n")
		f.write(self.ep_mem_high_wat + "\n")
		f.write(self.ep_item_commit_failed + "\n")
		f.write(self.ep_cache_miss_rate + "\n")
		f.write(self.couch_docs_fragmentation + "\n")
		f.write(self.ep_diskqueue_fill + "\n")
		f.write(self.ep_diskqueue_drain + "\n")
		f.write(self.evictions + "\n")
		f.write(self.memUsedPercentOfLowWatMark + "\n")
		f.write(self.memUsedPercentOfHighWatMark + "\n")
		f.write(self.ep_diskqueue_fill_drain_ratio + "\n")
		for curNode in self.nodes:
			curNode.outputNode(f)
		f.close()