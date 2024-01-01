class Bucket:
    def __init__(self, attributes_list):
        self.attribute_map  = {}
        self.attributes_list = attributes_list
        self.name = attributes_list[unicode("name")].encode("utf-8")
        self.attribute_map["name"] = self.name
        self.stats_uri = attributes_list[unicode("stats")][unicode("uri")]
        self.attribute_map["stats_uri"] = self.stats_uri
