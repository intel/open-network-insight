#!/bin/env python

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from multiprocessing import Process

import time
import os

from oni.utils import Util

class flow_ingest(object):



	def __init__(self,conf):

		self._initialize_members(conf)

	def _initialize_members(self,conf):

		self._collector_path = None
                self._hdfs_root_path = None
                self._queue_name = None
                self._dsource = 'flow'

		# valdiate configuration info.
                conf_err_msg = "Please provide a valid '{0}' in the configuration file"
                Util.validate_parameter(conf['collector_path'],conf_err_msg.format("collector_path"))
                Util.validate_parameter(conf['queue_name'],conf_err_msg.format("queue_name"))

               	# set configuration.
		self._collector_path = conf['collector_path']
                self._hdfs_root_path = "{0}/{1}".format(os.getenv('HUSER','/user/oni') , self._dsource)
               	self._queue_name = conf['queue_name']


	def start(self):


    		print self._collector_path

    		# start watch-dog.
    		event_handler = new_file(self)
    		observer = Observer()
    		observer.schedule(event_handler,self._collector_path)
   	 	observer.start()

    		try:
        		while True:
            			time.sleep(1)
    		except KeyboardInterrupt:
        		observer.stop()
			observer.join()

	def load_new_file(self,file):

		# create new process for the new file.

		print file
		if not  ".current" in file:
			p = Process(target=self._load_to_hdfs, args=(file,))
			p.start()
			p.join()

	def _load_to_hdfs(self,file):

		# get file name and date
		binary_year,binary_month,binary_day,binary_hour,binary_date_path,file_name =  Util.build_hdfs_path(file,'flow')

		# hdfs path with timestamp.
                hdfs_path = "{0}/{1}/{2}".format(self._hdfs_root_path,binary_date_path,binary_hour)
                Util.creat_hdfs_folder(hdfs_path)

		# load to hdfs.
		Util.load_to_hdfs(file_name,file,hdfs_path)

		# send the notification to rabbitmq server.
		hadoop_pcap_file = "{0}/{1}".format(hdfs_path,file_name)
                Util.send_new_file_notification(hadoop_pcap_file,self._queue_name)

		print "Done !!!!!"

class new_file(FileSystemEventHandler):

	_flow_instance = None
	def __init__(self,flow_class):
		self._flow_instance = flow_class

	def on_moved(self,event):

		if not event.is_directory:
			self._flow_instance.load_new_file(event.dest_path)

    def on_created(self,event):
        if not event.is_directory:
            self._flow_instance.load_new_file(event.src_path)



