#!/bin/env python

import json
import os
import subprocess

class kerberos(object):

	_script_path = None
	_conf_file = None
	_kerberos_conf = None
	_kinit = None
	_kinitopts = None
	_keytab = None
	_krb_user = None
	_kinit_args = None

	def __init__(self):

		#self._script_path = os.path.dirname(os.path.abspath(__file__))
		#self._conf_file = "{0}/etc/kerberos.json".format(self._script_path)
		#print self._conf_file
		#self._kerberos_conf = json.loads(open (self._conf_file).read())

		self._kinit =  os.environ['KINITPATH']
		self._kinitopts =  os.environ['KINITOPTS']
		self._keytab =  os.environ['KEYTABPATH']
		self._krb_user =  os.environ['KRB_USER']

		self._kinit_args = [self._kinit,self._kinitopts,self._keytab,self._krb_user]

	def authenticate(self):	

		kinit = subprocess.Popen(self._kinit_args, stderr = subprocess.PIPE)
		output,error = kinit.communicate()
		if not kinit.returncode == 0:
			if error:
				print error.rstrip()
				sys.exit(kinit.returncode)
		print "Successfully authenticated!"

		
