#!/usr/bin/env python
class NodeOutput:

	def __init__(self, nodeName):
		self.nodeName = nodeName
		self.hostName = "\"" + nodeName + "\""
		self.clusterMembership = "\"" + nodeName + ":clusterMembership\""
		self.status = "\"" + nodeName + ":status\""
		self.memoryFree = "\"" + nodeName + ":memoryFree\""
		self.memoryTotal = "\"" + nodeName + ":memoryTotal\""
		self.mcdMemoryAllocated = "\"" + nodeName + ":mcdMemoryAllocated\""
		self.mcdMemoryReserved = "\"" + nodeName + ":mcdMemoryReserved\""
		self.cpu_utilization_rate = "\"" + nodeName + ":cpu_utilization_rate\""
		self.swap_total = "\"" + nodeName + ":swap_total\""
		self.swap_used = "\"" + nodeName + ":swap_used\""
		self.cmd_get = "\"" + nodeName + ":cmd_get\""
		self.couch_docs_actual_disk_size = "\"" + nodeName + ":couch_docs_actual_disk_size\""
		self.couch_docs_data_size = "\"" + nodeName + ":couch_docs_data_size\""
		self.couch_views_actual_disk_size = "\"" + nodeName + ":couch_views_actual_disk_size\""
		self.couch_views_data_size = "\"" + nodeName + ":couch_views_data_size\""
		self.curr_items = "\"" + nodeName + ":curr_items\""
		self.curr_items_tot = "\"" + nodeName + ":curr_items_tot\""
		self.ep_bg_fetched = "\"" + nodeName + ":ep_bg_fetched\""
		self.get_hits = "\"" + nodeName + ":get_hits\""
		self.mem_used = "\"" + nodeName + ":mem_used\""
		self.ops = "\"" + nodeName + ":ops\""
		#self.vb_active_num_non_resident = "\"" + nodeName + ":vb_active_num_non_resident\""
		#self.vb_replica_curr_items = "\"" + nodeName + ":vb_replica_curr_items\""
		self.includesKv = False
	
	def addTimeSlice(self, nodeTimeSlice, timeslice):
		if nodeTimeSlice.hostname == self.nodeName:
			if nodeTimeSlice.timeslice == timeslice:
				self.hostName += ","
				self.clusterMembership += ",\"" + nodeTimeSlice.attribute_map["clusterMembership"] + "\""
				self.status += ",\"" + nodeTimeSlice.attribute_map["status"] + "\""
				self.memoryFree += "," + str(nodeTimeSlice.attribute_map["memoryFree"])
				self.memoryTotal += "," + str(nodeTimeSlice.attribute_map["memoryTotal"])
				self.mcdMemoryAllocated += "," + str(nodeTimeSlice.attribute_map["mcdMemoryAllocated"])
				self.mcdMemoryReserved += "," + str(nodeTimeSlice.attribute_map["mcdMemoryReserved"])
				self.cpu_utilization_rate += "," + str(nodeTimeSlice.attribute_map["cpu_utilization_rate"])
				self.swap_total += "," + str(nodeTimeSlice.attribute_map["swap_total"])
				self.swap_used += "," + str(nodeTimeSlice.attribute_map["swap_used"])
				self.includesKv = nodeTimeSlice.includesKv
				if nodeTimeSlice.includesKv:
					self.cmd_get += "," + str(nodeTimeSlice.attribute_map["cmd_get"])
					self.couch_docs_actual_disk_size += "," + str(nodeTimeSlice.attribute_map["couch_docs_actual_disk_size"])
					self.couch_docs_data_size += "," + str(nodeTimeSlice.attribute_map["couch_docs_data_size"])
					self.couch_views_actual_disk_size += "," + str(nodeTimeSlice.attribute_map["couch_views_actual_disk_size"])
					self.couch_views_data_size += "," + str(nodeTimeSlice.attribute_map["couch_views_data_size"])
					self.curr_items += "," + str(nodeTimeSlice.attribute_map["curr_items"])
					self.curr_items_tot += "," + str(nodeTimeSlice.attribute_map["curr_items_tot"])
					self.ep_bg_fetched += "," + str(nodeTimeSlice.attribute_map["ep_bg_fetched"])
					self.get_hits += "," + str(nodeTimeSlice.attribute_map["get_hits"])
					self.mem_used += "," + str(nodeTimeSlice.attribute_map["mem_used"])
					self.ops += "," + str(nodeTimeSlice.attribute_map["ops"])
					#self.vb_active_num_non_resident += "," + str(nodeTimeSlice.attribute_map["vb_active_num_non_resident"])
					#self.vb_replica_curr_items += "," + str(nodeTimeSlice.attribute_map["vb_replica_curr_items"])

		
	def outputNode(self, f):
		f.write(self.hostName + "\n")
		f.write(self.clusterMembership + "\n")
		f.write(self.status + "\n")
		f.write(self.memoryFree + "\n")
		f.write(self.memoryTotal + "\n")
		f.write(self.mcdMemoryAllocated + "\n")
		f.write(self.mcdMemoryReserved + "\n")
		f.write(self.cpu_utilization_rate + "\n")
		f.write(self.swap_total + "\n")
		f.write(self.swap_used + "\n")
		if self.includesKv:
			f.write(self.cmd_get + "\n")
			f.write(self.couch_docs_actual_disk_size + "\n")
			f.write(self.couch_docs_data_size + "\n")
			f.write(self.couch_views_actual_disk_size + "\n")
			f.write(self.couch_views_data_size + "\n")
			f.write(self.curr_items + "\n")
			f.write(self.curr_items_tot + "\n")
			f.write(self.ep_bg_fetched + "\n")
			f.write(self.get_hits + "\n")
			f.write(self.mem_used + "\n")
			f.write(self.ops + "\n")
			#f.write(self.vb_active_num_non_resident + "\n")
			#f.write(self.vb_replica_curr_items + "\n")

		