from pysys.basetest import BaseTest as PysysBaseTest
from elasticsearch import Elasticsearch

import os
from datetime import datetime

class PySysTest(PysysBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		PysysBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):
		elastic_conn_str = [self.project.ELASTIC_URL]
		es = Elasticsearch(elastic_conn_str)

		doc = {
			'author': 'kimchy',
			'text': 'Elasticsearch: cool. bonsai cool.',
			'timestamp': datetime.now(),
		}

		index_name = 'mongosearch2'
		doc_type = 'test'
		doc_id = 14

		es.indices.delete(index=index_name, ignore=[400, 404])
		res = es.index(index=index_name, doc_type=doc_type, id=doc_id, body=doc, refresh=True)
		self.log.info(res['result'])

		res = es.index(index=index_name, doc_type=doc_type, id=doc_id + 1, body=doc, refresh=True)
		self.log.info(res['result'])

		res = es.get(index=index_name, doc_type=doc_type, id=doc_id)
		self.log.info(res['_source'])

		es.indices.refresh(index=index_name)

		res = es.search(index=index_name, body={"query": {"match_all": {}}})
		self.log.info("Got %d Hits:" % res['hits']['total'])
		for hit in res['hits']['hits']:
			self.log.info("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

	def validate(self):
		self.assertGrep('run.log', expr='bonsai cool')

