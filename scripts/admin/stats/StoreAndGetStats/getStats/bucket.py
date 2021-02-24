import json
class Bucket(object):
    def __init__(self, attributes_list):
        self.attribute_map  = {}
        self.attributes_list = attributes_list
        self.name = attributes_list[unicode("name")].encode("utf-8")
        self.attribute_map["name"] = self.name
        self.quotaPercentUsed = attributes_list[unicode("basicStats")][unicode("quotaPercentUsed")]
        self.attribute_map["quotaPercentUsed"] = self.quotaPercentUsed
        self.itemCount = attributes_list[unicode("basicStats")][unicode("itemCount")]
        self.attribute_map["itemCount"] = self.itemCount
        self.memUsed = attributes_list[unicode("basicStats")][unicode("memUsed")]
        self.attribute_map["memUsed"] = self.memUsed
        self.stats_uri = attributes_list[unicode("stats")][unicode("uri")]
        self.attribute_map["stats_uri"] = self.stats_uri

    def setStats(self, stats):
        self.stats = stats[unicode("op")][unicode("samples")]

        #create a baseline for ep_bg_fetched (int type) should be close to zero
        self.attribute_map["ep_bg_fetched"]  = self.stats[unicode("ep_bg_fetched")][0]
        #create a baseline for vb_active_resident_items_ratio (float type)
        self.attribute_map["vb_active_resident_items_ratio"] = self.stats[unicode("vb_active_resident_items_ratio")][0]
        #create a baseline for vb_replica_resident_items_ratio (float type)
        self.attribute_map["vb_replica_resident_items_ratio"] = self.stats[unicode("vb_replica_resident_items_ratio")][0]
        #create a baseline for ep_tmp_oom_errors (int type)
        self.attribute_map["ep_tmp_oom_errors"] = self.stats[unicode("ep_tmp_oom_errors")][0]
        #any values greater than 0 for ep_oom_errors mean bucket has exceeded its total mem aloc, critical error
        self.attribute_map["ep_oom_errors"] = self.stats[unicode("ep_oom_errors")][0]
        #create a baseline for ep_queue_size (int type)
        self.attribute_map["ep_queue_size"] = self.stats[unicode("ep_queue_size")][0]
        #create a baseline for ep_flusher_todo
        self.attribute_map["ep_flusher_todo"] = self.stats[unicode("ep_flusher_todo")][0]
        #create a baseline for ep_dcp_views_items_remaining + ep_dcp_2i_items_remaining (ints)
        self.attribute_map["ep_dcp_views_items_remaining"] = self.stats[unicode("ep_dcp_views_items_remaining")][0]
        self.attribute_map["ep_dcp_2i_items_remaining"] = self.stats[unicode("ep_dcp_2i_items_remaining")][0]
        self.attribute_map["ep_dcp_views_items_remaining+ep_dcp_2i_items_remaining"] = self.attribute_map["ep_dcp_views_items_remaining"] + self.attribute_map["ep_dcp_2i_items_remaining"]
        # If vb_replica_num falls below (1024 * the
        # number of configured replicas) / the
        # number of servers, it indicates that a
        # rebalance is required.
        self.attribute_map["vb_replica_num"] = self.stats[unicode("vb_replica_num")][0]
        # vb_active_num should always equal 1024 /
        # the number of servers. If it does not,
        # it indicates a node failure and that a
        # failover + rebalance is required.
        self.attribute_map["vb_active_num"] = self.stats[unicode("vb_active_num")][0]
        self.attribute_map["ep_mem_low_wat"] = self.stats[unicode("ep_mem_low_wat")][0]
        self.attribute_map["ep_mem_high_wat"] = self.stats[unicode("ep_mem_high_wat")][0]
        self.attribute_map["ep_item_commit_failed"] = self.stats[unicode("ep_item_commit_failed")][0]

        self.attribute_map["ep_cache_miss_rate"] = self.stats[unicode("ep_cache_miss_rate")][0]
        self.attribute_map["couch_docs_fragmentation"] = self.stats[unicode("couch_docs_fragmentation")][0]
        self.attribute_map["ep_diskqueue_fill"] = self.stats[unicode("ep_diskqueue_fill")][0]
        self.attribute_map["ep_diskqueue_drain"] = self.stats[unicode("ep_diskqueue_drain")][0]
        self.attribute_map["evictions"] = self.stats[unicode("evictions")][0]


        #Calculate memory useage using memUsed and High/Low water mark
        memUsedPercentOfLowWatMark = float(self.memUsed)/float(self.attribute_map["ep_mem_low_wat"])
        memUsedPercentOfHighWatMark = float(self.memUsed)/float(self.attribute_map["ep_mem_high_wat"])
        self.attribute_map["memUsedPercentOfLowWatMark"] = memUsedPercentOfLowWatMark * 100.00
        self.attribute_map["memUsedPercentOfHighWatMark"] = memUsedPercentOfHighWatMark * 100.00

        fill_avg = sum(self.stats[unicode("ep_diskqueue_fill")])/float(len(self.stats[unicode("ep_diskqueue_fill")]))
        drain_avg = sum(self.stats[unicode("ep_diskqueue_drain")])/float(len(self.stats[unicode("ep_diskqueue_drain")]))
        ep_diskqueue_fill_drain_ratio = fill_avg
        if drain_avg != 0.0:
            ep_diskqueue_fill_drain_ratio = fill_avg/drain_avg

        self.attribute_map["ep_diskqueue_fill_drain_ratio"] = ep_diskqueue_fill_drain_ratio
        self.attribute_map["ep_dcp_total_queue"] = 0
        self.attribute_map["ep_io_num_write"] = 0
        self.attribute_map["ep_io_num_read"] = 0



