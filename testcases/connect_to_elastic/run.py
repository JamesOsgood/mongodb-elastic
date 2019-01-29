from MongoDBElasticBaseTest import MongoDBElasticBaseTest

import os

class PySysTest(MongoDBElasticBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		MongoDBElasticBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):
		pass

	def validate(self):
		pass

