from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
class CBCluster(object):
	def __init__(self, attributes_list, ipaddr):
		cluster = Cluster('couchbase://' + ipaddr)
		authenticator = PasswordAuthenticator(attributes_list[unicode("user")], attributes_list[unicode("pwd")])
		cluster.authenticate(authenticator)
		self.bucket = cluster.open_bucket(attributes_list[unicode("bucket")])
		
	def upsert(self, doc_key, json_data):
		self.bucket.upsert(doc_key, json_data)
		
	def get(self, doc_key):
		try:
			rv = self.bucket.get(doc_key)
			return rv
		except:
			return ""