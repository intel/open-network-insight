#!/bin/env python


"""
    Master Ingest Framewokr.
"""

import argparse
import json
import os
import sys

from oni.dns_master import dns_ingest
from oni.flow_master import flow_ingest
from oni.kerberos import Kerberos


script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/etc/master_ingest.json".format(script_path)
ingest_conf = json.loads(open (conf_file).read())


def main():

	# Input Parameters
	parser = argparse.ArgumentParser(description="Master Ingest Framework")
	parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow)',metavar='')
	args = parser.parse_args()

	start_ingest(args.type)


def start_ingest(data_type):

	ingest = None

	if os.getenv('KRB_AUTH'):
                kb = Kerberos()
                kb.authenticate()

	if data_type == "dns":
		ingest = dns_ingest(ingest_conf['dns'])
	elif data_type == "flow":
		ingest = flow_ingest(ingest_conf['flow'])
	else:
		print "Ingest type '{0}' is not supported".format(data_type)
                sys.exit(1)

	# start the ingest.
	ingest.start()


if __name__ == '__main__':
	main()
