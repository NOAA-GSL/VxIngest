#!/usr/bin/env python
class Node:

    def __init__(self, attributes_dict, timeslice):
        self.attributes_dict = attributes_dict
        self.attribute_map = {}

        self.attribute_map["clusterMembership"] = attributes_dict["clusterMembership"].encode("utf-8")
        self.hostname = attributes_dict["hostname"].encode("utf-8")
        self.timeslice = timeslice
        self.includesKv = False
        for service in attributes_dict["services"]:
        	if service == "kv":
        		self.includesKv = True
        self.attribute_map["hostname"] = self.hostname
        self.attribute_map["status"] = attributes_dict["status"].encode("utf-8")
        self.attribute_map["memoryFree"] = attributes_dict["memoryFree"]
        self.attribute_map["memoryTotal"] = attributes_dict["memoryTotal"]
        self.attribute_map["mcdMemoryAllocated"] = attributes_dict["mcdMemoryAllocated"]
        self.attribute_map["mcdMemoryReserved"] = attributes_dict["mcdMemoryReserved"]
        self.attribute_map["cpu_utilization_rate"] = attributes_dict["systemStats"]["cpu_utilization_rate"]
        self.attribute_map["swap_total"] = attributes_dict["systemStats"]["swap_total"]
        self.attribute_map["swap_used"] = attributes_dict["systemStats"]["swap_used"]
        if self.includesKv:
	        self.attribute_map["cmd_get"] = attributes_dict["interestingStats"]["cmd_get"]
	        self.attribute_map["couch_docs_actual_disk_size"] = attributes_dict["interestingStats"]["couch_docs_actual_disk_size"]
	        self.attribute_map["couch_docs_data_size"] = attributes_dict["interestingStats"]["couch_docs_data_size"]
	        self.attribute_map["couch_views_actual_disk_size"] = attributes_dict["interestingStats"]["couch_views_actual_disk_size"]
	        self.attribute_map["couch_views_data_size"] = attributes_dict["interestingStats"]["couch_views_data_size"]
	        self.attribute_map["curr_items"] = attributes_dict["interestingStats"]["curr_items"]
	        self.attribute_map["curr_items_tot"] = attributes_dict["interestingStats"]["curr_items_tot"]
	        self.attribute_map["ep_bg_fetched"] = attributes_dict["interestingStats"]["ep_bg_fetched"]
	        self.attribute_map["get_hits"] = attributes_dict["interestingStats"]["get_hits"]
	        self.attribute_map["mem_used"] = attributes_dict["interestingStats"]["mem_used"]
	        self.attribute_map["ops"] = attributes_dict["interestingStats"]["ops"]
	        self.attribute_map["vb_active_num_non_resident"] = attributes_dict["interestingStats"]["vb_active_num_non_resident"]
	        self.attribute_map["vb_replica_curr_items"] = attributes_dict["interestingStats"]["vb_replica_curr_items"]

