import os
from jco.test.utils.BaseTest import BaseTest

class MongoDBElasticBaseTest(BaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)


	def connectToElastic(self, connection_string):
		pass