#!/user/bin/python

import numpy as np
import csv
#import statsmodels.tsa.stattools as st
import pickle
import collections
import linecache
import sys

k = 20 #number of topics
tt = 6 # number of time intervals
rpath = sys.argv[1]
wsdict = rpath +'widx_dict.pkl'
dsdict = rpath +'docidx_dict.pkl'
tdmfile= rpath +'doc_wc.dat'
wfile = rpath +'words.dat'
dfile = rpath +'doc.dat'
mfile = rpath +'model.dat'


word_dict = True
doc_line = True
dump_doc_line = True
durable = False


# build dictionary of all words  - key is word, value is array [index,wc]
# increment unique wc ++
if word_dict:
	with open(tdmfile, 'r') as csvfile:
		rowct = 1
		w_idx = 1
		wcdict = {}
		readr = csv.reader(csvfile, delimiter=',', quotechar='"')
		next(readr) #is there a header?
		with open(wfile, 'w') as f:
			for row in readr:
				if row[1] not in wcdict:
					wcdict[row[1]] = w_idx
					f.write("%s,%s\n" %(w_idx,row[1]) )
					w_idx += 1
				rowct += 1
				if rowct%1000000 == 0:
					print(rowct,"rows processed")
				#if rowct == 100000:
					#print(wcdict)
					#break

	if durable:
		output = open(wsdict, 'wb')
		pickle.dump(wcdict, output)
		output.close()

if doc_line:
	if durable:
		pkl_file = open(wsdict, 'rb')
		wcdict = pickle.load(pkl_file)
		pkl_file.close()
	with open(dfile, 'w') as f:
		rowct = 1
		doc_idx = 1
		docdict = {}
		with open(tdmfile, 'r') as csvfile:
			readr = csv.reader(csvfile, delimiter=',', quotechar='"')
			next(readr)		
			for row in readr:
				if row[0] in docdict:
					docdict[row[0]][1] += 1
					docdict[row[0]][2].append(" %s:%s" % (wcdict[row[1]],row[2]) )
				else:
					docdict[row[0]] = [doc_idx,1,[]]
					f.write("%s,%s\n" %(doc_idx,row[0]) )
					doc_idx += 1
					docdict[row[0]][2].append(" %s:%s" % (wcdict[row[1]],row[2]) )
				rowct += 1
				if rowct%1000000 == 0:
					#print(docdict[row[0]])
					print(rowct,"rows processed")
	wcdict = None
	if durable:
		output = open(dsdict, 'wb')
		pickle.dump(docdict, output)
		output.close()
	
if dump_doc_line:
	if durable:
		pkl_file = open(dsdict, 'rb')
		docdict = pickle.load(pkl_file)
		pkl_file.close()
	with open(mfile, 'w') as f:
		with open(dfile, 'r') as csvfile:
			readr = csv.reader(csvfile, delimiter=',', quotechar='"')
			print("writing ...")
			for row in readr:			
				f.write("%s%s\n" %(docdict[row[1]][1],''.join(docdict[row[1]][2]) ) )
 
