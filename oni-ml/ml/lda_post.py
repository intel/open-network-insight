import numpy as np
import pickle
import csv
import linecache
import sys

print "+++++++++++++++++++"
print "modules loaded"
print "+++++++++++++++++++"

# Input files: doc.dat is list of unique docs, words.dat is list of unique words
# final.gamma is output of topic distributions per doc, final.beta is output of
# distribution of words per topic (originally 20x#words, transform this to end
# up #wordsx20)
rpath = sys.argv[1]
doc_file = rpath+ 'doc.dat'
topic_file = rpath+'final.gamma'
word_prob = rpath+'final.beta'
word_file = rpath+'words.dat'
dresults = rpath+'doc_results.csv'
wresults = rpath+'word_results.csv'
dsdict = rpath+'docidx_dict.pkl'

topics = True
words = True

if topics:

	#docs = np.loadtxt(doc_file, np.str, delimiter=",")
	#pkl_file = open(dsdict, 'rb')
	#docs = pickle.load(pkl_file)
	#print "Pickle file loaded",len(docs)

	#topics = np.loadtxt(topic_file, np.float)
	with open(topic_file, 'r') as csvfile:
	# Generate file with correctly formatted topic info per document
		readr = csv.reader(csvfile, delimiter=' ', quotechar='"')
		#next(readr)
		print "Generating topic distribution per doc..."
		j = 1
		f = open(dresults, "w")
		fmt = ['%s '] * 20 +  ['%s\n']
		format = ''.join(fmt)
		for row in readr:		
			t = np.asarray(row,dtype=np.float64)
			total_t = sum(t)
			#doc = np.atleast_2d(docs[row,1])
			#doc_topics[row,:1] = doc
			if total_t > 0:
				norm_t = map(str, [ti/total_t for ti in t])
				norm_t = ' '.join(norm_t)
				#doc_topics[row,1] = norm_t
			else:
				norm_t = "0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0"
			
			doc = linecache.getline(doc_file,j).split(',')[1].replace('\n','')
			
			line = str("%s,%s\n" % (doc,norm_t )).encode('ascii')
			f.write(line)
			if j % 1000000 == 0:
				print j,"rows processed"
			j += 1

	print "Saved topic distribution in doc_results.csv"
			#np.savetxt('doc_results.csv', doc_topics, delimiter=",", fmt="%s")


if words:
	# Generate word file
	words = np.loadtxt(word_prob, np.float64)
	word_list = np.loadtxt(word_file, np.str, delimiter=",")
	print word_list.shape[0]
	print words.shape

	print "Generating probability of word given topic, p(w|z)..."

	# Put words in correct format
	wl_arr = np.empty([word_list.shape[0]+1, 1], dtype="S20")
	row = 0
	for wl in word_list:
	    wl = '_'.join(wl[1:])
	    wl_arr[row] = wl
	    row += 1
	wl_arr[row] = "0_0_0_0_0"


	# Normalize p(w|z) and flip so each line => word instead of each line => topic
	p_wgz = np.empty([words.shape[1], words.shape[0]], dtype=np.float64)
	col = 0
	for w in words:
	    raw_w = [np.exp(wi) for wi in w]
	    total_w = sum(raw_w)
	    norm_w = [rwi/total_w for rwi in raw_w]
	    #print np.amax(norm_w)
	    p_wgz[:,col] = norm_w
	    col += 1

	# Format each row of probability array (p(w|z))
	#p_arr = np.empty([p_wgz.shape[0], 1], dtype="S5000")
	#row = 0
	#for pw in p_wgz.astype(np.str):
	#    p_row = "\"" + ' '.join(pw) + "\""
	#    p_arr[row] = p_row
	#    row += 1

	# Combine word list and probability array, write file
	#p_arr = np.append(wl_arr, p_arr, axis=1)
	print "Writing p(w|z) to pword_given_topic.csv"
	#np.savetxt('pword_given_topic.csv', p_arr, delimiter = ",", fmt="%s")

	f = open(wresults, "w")

	#np.savetxt(f, qties,fmt="%15u")
	#fmt ='%1u'
	fmt = ['%s '] * 19 +  ['%s\n']
	format = ''.join(fmt)
	#print format
	for j in xrange(0,len(p_wgz)):
		line1 =  str("%s," % (wl_arr[j][0]) ).encode('ascii')
		f.write(line1)
		line = str(format % (tuple(p_wgz[j]) ) ).encode('ascii')
		f.write(line)

