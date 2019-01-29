import os
import copy
import uuid
import hashlib
from datetime import datetime

from backup_sink.FolderBackupSink import FolderBackupSink
from backup_sink.S3BackupSink import S3BackupSink

from pymongo import MongoClient

class BackupFolder(object):
	def __init__ (self, folder, create_id_file=True):
		self.folder = folder
		self.backup_id = self.getBackupId(folder, create_id_file)

	def getBackupId(self, folder, create_id_file):
		# lookup backup id
		backup_file = os.path.join(folder, '.backup_id')
		backup_id = None
		try:
			with open(backup_file) as bf:
				backup_id = bf.readline()
		except:
			pass

		if not backup_id and create_id_file:
			# Generate a backup id 
			backup_id = str(uuid.uuid4())
			with open(backup_file, "w") as bf:
				bf.write(backup_id)
		
		return backup_id

class BackupAudio(object):
	def __init__ (self, parent, conn_str, collection_name='audio_backup'):
		self.parent = parent
		self.conn_str = conn_str
		self.collection_name = collection_name
		self.initSinkFactory()

	# Logging
	def log_info(self, message):
		self.parent.log.info(message)

	def log_error(self, message):
		self.parent.log.error(message)

	def initSinkFactory(self):
		self.sinkFactory = {
			FolderBackupSink.getProviderName() : FolderBackupSink.createSink,
			S3BackupSink.getProviderName() : S3BackupSink.createSink
		}

	def createSink(self, name, state ):
		factory = self.sinkFactory[name]
		return factory(self, state)

	def restore_folder(self, folder, force_restore=False, credentials=None):
		
		# Look for backup id file
		bf = BackupFolder(folder, create_id_file=False)
		# If it is backed up, restore it
		if bf.backup_id:
			self.log_info("Restoring folder %s" % folder)
			# Connect to mongo
			db = MongoClient(self.conn_str).get_default_database()
			# See whether we need to backup this file
			for backed_up_file in db.get_collection(self.collection_name).find( { 'backup_id' : bf.backup_id }):
				# See if file already exists
				file_details = backed_up_file['file']
				fullpath = os.path.join(folder, file_details['filename'])
				restore_file = True
				if os.path.exists(fullpath):
					if force_restore:
						os.path.remove(fullpath)
					else:
						# Check md5 sum
						# for now, remove
						# os.remove(fullpath)
						restore_file = False
						self.log_info("%s already exists" % fullpath)
				
				if restore_file:
					# Create sink
					provider = backed_up_file['provider']
					sink = self.createSink(provider['name'], provider['instance_details'], credentials)

					# Restore file
					sink.restore_file(bf, file_details['filename'], fullpath)

		# Recurse dirs
		for item in os.listdir(folder):
			fullpath = os.path.join(folder, item)	
			if os.path.isdir(fullpath) and not item.startswith('.'):
				self.restore_folder(fullpath, force_restore )

	def backup_folder(self, folder, backup_sink):
		# get list of files
		filesToBackup = []
		extensions = ['.wav', '.mp3']
		self.getFilesToProcess(folder, filesToBackup, extensions)

		# Connect to mongo
		db = MongoClient(self.conn_str).get_default_database()
		
		provider_name = backup_sink.getProviderName()
		instance_details = backup_sink.getProviderInstanceDetails()
		
		for file in filesToBackup:
			bf = file[0]
			filename = file[1]
			orig_filename = file[2]
			
			# See whether we need to backup this file
			collection = db.get_collection(self.collection_name)
			backed_up_file = collection.find_one( { 'backup_id' : bf.backup_id, 
			                                             'file.filename' : filename,
														 'provider.name' : provider_name,
														 'provider.instance_details' : instance_details })
			if not backed_up_file:
				backup_sink.backup_file(bf, filename, orig_filename )
				# Write a record to mongo
				mtime = os.path.getmtime(orig_filename)
				record = { 'backup_id' : bf.backup_id, 
				           'provider' : 
						   		{ 'name' : provider_name,
						          'instance_details' : instance_details
								},
							'file' : { 
								'filename' : filename,
							    'orig_filename' : orig_filename,
								'hash' : self.md5(orig_filename)
							},
 						    'last_modified' : 
							 	{ 'dt' : datetime.fromtimestamp(mtime),
								  'epoch' : os.path.getmtime(orig_filename)
								 }
						}
				collection.insert(record)
			else:
				self.log_info("%s already backed up" % orig_filename)

	def md5(self, filename):
		hash_md5 = hashlib.md5()
		with open(filename, "rb") as f:
			for chunk in iter(lambda: f.read(4096), b""):
				hash_md5.update(chunk)
		return hash_md5.hexdigest()

	def getFilesToProcess(self, path, files, extensions):
		bf = None
		for item in os.listdir(path):
			fullpath = os.path.join(path, item)	
			if os.path.isdir(fullpath) and not item.startswith('.'):
				self.getFilesToProcess(fullpath, files, extensions)
			else:
				filename, fileext = os.path.splitext(fullpath)
				if fileext.lower() in extensions:
					# Make sure this folder is marked as backed up
					if not bf:
						bf = BackupFolder(path)
					files.append((bf, item, fullpath))

