#!/user/bin/python

import csv
import sys

infile='qtiles.tsv'
outfile=sys.argv[1]+'_qtiles'

with open(infile, 'r') as csvfile:
		readr = csv.reader(csvfile, delimiter='\t', quotechar='"')
		outstring = '0 '
		with open(outfile, 'w') as f:
			for row in readr:
					
					if row[0] == '|':

						outstring=outstring[:-1]
						outstring += ",0 "
					else:
						outstring += ("%s " %(row[0]) )
			f.write(outstring)
					
