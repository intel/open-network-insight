
import sys
import subprocess
import pika

class Util(object):

	@classmethod
	def validate_parameter(cls,parameter,message):
		if parameter == None or parameter == "":
			cls.logger.error(message)
			sys.exit(1)

	@classmethod
	def creat_hdfs_folder(cls,hdfs_path):

		hadoop_create_folder="hadoop fs -mkdir -p {0}".format(hdfs_path)
		print hadoop_create_folder
         	subprocess.call(hadoop_create_folder,shell=True)

	@classmethod
	def load_to_hdfs(cls,file_name,file_local_path,hdfs_path):

		# move nfcapd file to hadoop.
		hadoop_pcap_file = "{0}/{1}".format(hdfs_path,file_name)
		load_to_hadoop_script = "hadoop fs -moveFromLocal {0} {1}".format(file_local_path,hadoop_pcap_file)
		# load_to_hadoop_script = "hadoop fs -put {0} {1}".format(file_local_path,hadoop_pcap_file)
		print load_to_hadoop_script
		subprocess.call(load_to_hadoop_script,shell=True)


	@classmethod
	def send_new_file_notification(cls,file,queue_name):

		connection_send = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		channel = connection_send.channel()
		channel.queue_declare(queue=queue_name)
		channel.basic_publish(exchange='',routing_key=queue_name,body=file)
		connection_send.close()


	@classmethod
	def build_hdfs_path(cls,file,data_type):


		if data_type == 'flow':

			# get file name and date.
			file_name_parts = file.split('/')
			file_name = file_name_parts[len(file_name_parts)-1]

			file_date = file_name.split('.')[1]
			binary_year = file_date[0:4]
			binary_month = file_date[4:6]
			binary_day = file_date[6:8]
			binary_hour = file_date[8:10]
			binary_date_path = file_date[0:8]

			return binary_year, binary_month, binary_day, binary_hour, binary_date_path, file_name


		elif data_type == 'dns':

			# get file name and date
			file_name_parts = file.split('/')
			file_name = file_name_parts[len(file_name_parts)-1]

			binary_hour = file_name_parts[len(file_name_parts)-2]
			binary_date_path = file_name_parts[len(file_name_parts)-3]
			binary_year = binary_date_path[0:4]
			binary_month = binary_date_path[4:6]
			binary_day = binary_date_path[6:8]

			return binary_year, binary_month, binary_day, binary_hour, binary_date_path, file_name



