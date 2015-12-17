#!/bin/env python

import argparse
import pika
import sys
import os
import json
import datetime
import subprocess
from multiprocessing import Process

from oni.kerberos import Kerberos
from oni.utils import Util

script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/etc/worker_ingest.json".format(script_path)
worker_conf = json.loads(open (conf_file).read())

ingest_type = None

def main():


	# input parameters
        parser = argparse.ArgumentParser(description="Worker Ingest Framework")
        parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow)',metavar='')
        args = parser.parse_args()

	start_worker(args.type)

def start_worker(data_type):

	if data_type != "dns" and data_type != "flow":
		print "Ingest type '{0}' is not supported".format(data_type)
		sys.exit(1)
	else:
		global ingest_type
		ingest_type = data_type

	if os.getenv('KRB_AUTH'):
		kb = Kerberos()
		kb.authenticate()

	start_file_watcher(ingest_type)


def start_file_watcher(ingest_type):

	# connect to rabbitmq
	rabbitmq_server = worker_conf['rabbitmq_server']
	connection =  pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
	channel = connection.channel()

	queue_name = worker_conf[ingest_type]['queue_name']
        print "Listening server {0}, queue:{1}".format(rabbitmq_server,queue_name)

	channel.queue_declare(queue=queue_name)
	channel.basic_consume(new_file_found,queue=queue_name)
	channel.start_consuming()

def new_file_found(ch,method,properties,body):

	print " [x] Received %r" % (body,)
	print datetime.datetime.now()

	ch.basic_ack(delivery_tag=method.delivery_tag)
	p = Process(target=process_new_binary_file, args=(body,))
	p.start()
	p.join()

def process_new_binary_file(new_file):

	# get file from hdfs
	get_file_cmd = "hadoop fs -get {0} ../stage/.".format(new_file)
	print get_file_cmd
	subprocess.call(get_file_cmd,shell=True)

	# get file name and date
	binary_year,binary_month,binary_day,binary_hour,binary_date_path,file_name =  Util.build_hdfs_path(new_file,ingest_type)

	# build process cmd.
	post_process_cmd = None
	process_opt = worker_conf[ingest_type]['process_opt']
	if ingest_type == 'dns':
		post_process_cmd = "tshark -r ../stage/{0} {1} >> ../stage/{0}.csv".format(file_name,process_opt)
	elif ingest_type == 'flow':
		post_process_cmd = "nfdump -o csv -r ../stage/{0} {1} > ../stage/{0}.csv".format(file_name,process_opt)
        else:
            print "Unsupported ingest type"
            sys.exit(1)

	print post_process_cmd
	subprocess.call(post_process_cmd,shell=True)

	# create folder if it does not exist
	h_base_path = "{0}/{1}".format(os.getenv('HUSER','/user/oni'), ingest_type)
	h_csv_path = "{0}/csv".format(h_base_path)
	create_folder_cmd = "hadoop fs -mkdir -p {0}/y={1}/m={2}/d={3}/h={4}".format(h_csv_path,binary_year,binary_month,binary_day,binary_hour)
	print create_folder_cmd
	subprocess.call(create_folder_cmd,shell=True)

	#move to hdfs.
	upld_cmd = "hadoop fs -moveFromLocal ../stage/{0}.csv {1}/y={2}/m={3}/d={4}/h={5}/.".format(file_name,h_csv_path,binary_year,binary_month,binary_day,binary_hour)
	print upld_cmd
	subprocess.call(upld_cmd,shell=True)

	#make tmp folder in stage
        h_stage_timestamp = datetime.datetime.now().strftime('%M%S%f')[:-4]
	h_stage_path =  "{0}/stage/{1}".format(h_base_path,h_stage_timestamp)
	create_tmp_cmd = "hadoop fs -mkdir -p {0}".format(h_stage_path)
	print create_tmp_cmd
	subprocess.call(create_tmp_cmd,shell=True)

	#load to avro
	load_avro_cmd = "hive -hiveconf dbname={6} -hiveconf y={0} -hiveconf m={1} -hiveconf d={2} -hiveconf h={3} -hiveconf data_location='{4}' -f oni/load_{5}_avro_parquet.hql".format(binary_year,binary_month,binary_day,binary_hour,h_stage_path,ingest_type,os.getenv('DBNAME','default') )

	print load_avro_cmd
	subprocess.call(load_avro_cmd,shell=True)
	
	#run ml_train
	train_ml_cmd = "../ml/./ml_score.sh {1}/{2}.csv {3} {4}".format(h_stage_path,file_name,ingest_type,os.getenv('TOL','1e-6')

	print load_avro_cmd
	subprocess.call(load_avro_cmd,shell=True)
	
	#remove from stage
	rm_tmp_cmd = "hadoop fs -rm -R -skipTrash {0}".format(h_stage_path)
	print rm_tmp_cmd
	subprocess.call(rm_tmp_cmd,shell=True)
	
	

	#can this delete other files when all is running on the same edge server?
	rm_tmp = "rm ../stage/{0}*".format(file_name)
	subprocess.call(rm_tmp,shell=True)

	print datetime.datetime.now()

if __name__ == '__main__':
	main()


