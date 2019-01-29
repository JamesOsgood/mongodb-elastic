from pysys.basetest import BaseTest as PysysBaseTest
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import time

import os
from datetime import datetime
import sys, threading
# before anything else, configure the logger
from pysys import log, ThreadedStreamHandler, ThreadedFileHandler
import logging
import thread
from pysys.constants import *
import random


class PySysTest(PysysBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		PysysBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):
		self.thread_started = False
		self.records_to_receive = 150
		
		# Make sure db / collection exist
		connStr = self.project.MONGODB_CONNECTION_STRING_ATLAS
		client = MongoClient(host=connStr)

		db = client.get_default_database()
		coll = db.get_collection('test_coll')

		# insert a doc
		key_index = 0
		coll.insert_one({"key" : "key_" + str(key_index), "time" : time.clock()})

		# Start thread to listen to notifications
		threadArgs = [connStr]
		self.startThread("ChangeStreamListener", self.change_stream_listen, threadArgs)
		while not self.thread_started:
			self.log.info("Waiting for thread to start")
			self.wait(1)

		# wait
		records_to_insert = self.records_to_receive
		while records_to_insert > 0:
			key_index += 1
			wait_period = random.uniform(0.1, 1.0)
			self.log.info("PRODUCER: %d" % key_index)
			coll.insert_one({"key" : "key_" + str(key_index), "time" : time.clock(), "wait_period" : wait_period})
			records_to_insert -= 1
			self.wait(wait_period)

		while self.records_to_receive > 0:
			self.log.info(self.records_to_receive)
			self.wait(1)
	def validate(self):
		pass

	def change_stream_listen(self, log, args):
		connStr = args[0]
		log.info("Listen and log")
		# Connect to mongo
		client = MongoClient(host=connStr)
		db = client.get_default_database()
		coll = db.get_collection('test_coll')

		total = 0.0
		count = 0

		# Connect to elastic
		# elastic_conn_str = [self.project.ELASTIC_URL]
		# es = Elasticsearch(elastic_conn_str)

		# index_name = 'mongosearch'
		# doc_type = 'test'
		# es.indices.delete(index=index_name, ignore=[400, 404])

		self.thread_started = True
		try:
			for changed_doc in coll.watch(
					[{'$match': {'operationType': 'insert'}}]):

				# doc = changed_doc['fullDocument']
				# doc.pop('_id')
				# res = es.index(index=index_name, doc_type=doc_type, id=count, body=doc)
				# count += 1
				# log.info(res['result'])

				self.records_to_receive -=1
				log.info(self.records_to_receive)

		except PyMongoError as ex:
			# The ChangeStream encountered an unrecoverable error or the
			# resume attempt failed to recreate the cursor.
			print(ex)

	def startThread(self, name, threadproc, args ):
		threadArgs = (name, threadproc, args)
		thread.start_new_thread(self.threadProcLocal, threadArgs)
		
	# Local thread proc - sets up the logger
	def threadProcLocal(self, name, threadproc, args):
		log = self.createLogger(name)
		# Call threadproc
		threadproc(log, args)

	# Create a logger that logs to stdout and run.log from this thread
	def createLogger(self, name):
		# Create a logger for this thread
		log = logging.getLogger(name)
		# The root logger log level (set to DEBUG as all filtering is done by the handlers)."""
		log.setLevel(logging.DEBUG)
		# Add custom handler to pick up this thread
		stdoutHandler = ThreadedStreamHandler(sys.stdout)
		stdoutHandler.setFormatter(PROJECT.formatters.stdout)
		stdoutHandler.setLevel(logging.INFO)
		log.addHandler(stdoutHandler)

		# Log to run.log as well
		testFileHandler = ThreadedFileHandler(os.path.join(self.output, 'run.log'))
		testFileHandler.setFormatter(PROJECT.formatters.runlog)
		testFileHandler.setLevel(logging.INFO)
		log.addHandler(testFileHandler)
		return log

	def validate(self):
		pass

