from MongoDBElasticBaseTest import MongoDBElasticBaseTest
from elasticsearch import Elasticsearch

import os
from datetime import datetime

class PySysTest(MongoDBElasticBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		MongoDBElasticBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):
		es = Elasticsearch()

		doc = {
			'author': 'kimchy',
			'text': 'Elasticsearch: cool. bonsai cool.',
			'timestamp': datetime.now(),
		}
		res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
		print(res['result'])

		res = es.get(index="test-index", doc_type='tweet', id=1)
		print(res['_source'])

		es.indices.refresh(index="test-index")

		res = es.search(index="test-index", body={"query": {"match_all": {}}})
		print("Got %d Hits:" % res['hits']['total'])
		for hit in res['hits']['hits']:
			self.log.info("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

	def validate(self):
		pass

