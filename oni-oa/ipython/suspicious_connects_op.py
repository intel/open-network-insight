import socket
import struct
import csv
import numpy as np
import sys, getopt
from subprocess import call
from subprocess import check_output
def main():
    sdate = ''
    spath = '{0}/ipython/user/{1}/'
    scores_f = os.environ['DSOURCE']+"_scores.csv"
    userDir = ''
    topct = 500
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:u:',["help", "date=", "user="])
    except getopt.GetoptError as err:
        print 'usage: python sctipt.py -d <20150807> -u <duxbury>'
        sys.exit(2)
    for opt,arg in opts:
        if opt in ('-h', '--help'):
            print 'usage: python sctipt.py -d <20150807> -u <duxbury>'
            sys.exit()
        elif opt in ('-d', '--date'):
            sdate = arg
        elif opt in ('-u', '--user'):
            if arg[0] == '/':
                arg = arg[1:]    
            userDir = arg
    spath = spath.format(userDir, sdate)
    scores_full_path = spath + scores_f
    
    print "Creating Edge Files..."
    conns_list = []
    with open(scores_full_path, 'rb') as f:
        reader = csv.reader(f,delimiter=',') 
        reader.next();
        rowct = 1
        for row in reader:
            if int(row[0]) < 3: # 3 is the don't care
                # get src ip and dst ip
                sip = row[2]
                dip = row[3]
                # get hour and date 2014-07-08 10:10:40
                
                hr = row[1].split(' ')[1].split(':')[0]
                dy = row[1].split(' ')[0].split('-')[2] 
                mh = row[1].split(' ')[0].split('-')[1] 
                mm = row[1].split(' ')[1].split(':')[1]
                #TODO: using netflow_avro table, this query should change - no more minute(), hour()
                # also, there are more columns to return           
                conn = (sip,dip,dy,hr,mm)
                if conn not in conns_list:
                    conns_list.append(conn)                    
                if rowct == topct:
                    break
                rowct = rowct + 1
    for conn in conns_list:
        sip = conn[0]
        dip = conn[1]
        dy = conn[2]
        hr = conn[3]
        mm = conn[4]
    
        hivestr = (" \"set hive.cli.print.header=true; SELECT treceived as tstart,sip as srcip," +
        "dip as dstip,sport as sport,dport as dport,proto as proto,flag as flags,stos as TOS," +
        "ibyt as bytes, ipkt as pkts,input as input, output as output, rip as rip " +
         " from " + "os.environ['DBNAME']" + "." + os.environ['DSOURCE']  +  
        " WHERE ( (sip=\'" + sip + "\' AND dip=\'" + dip + "\') OR " +
        "(sip=\'" + dip + "\' AND dip=\'" + sip + "\') ) AND m="+mh+" AND d="+dy+" AND h="+hr +
        " AND trminute="+mm +" SORT BY unix_tstamp LIMIT 100; \"  > " + spath+ "edge-" + sip.replace(".","_") + "-" + 
        dip.replace(".","_") + "-" + hr + "-" + mm + ".tsv")
        
        print 'processing line ',rowct
        print hivestr
        #call(["hive","-S","-e", hivestr])
        check_output("hive -S -e " + hivestr, shell=True)

    print "Done Creating Edge Files..."

    print "\n Creating Chord Files..."
    srcdict = {}
    rowct = 1
    with open(scores_full_path, 'rb') as f:
        reader = csv.reader(f,delimiter=',') 
        reader.next();
        rowct = 1
        for row in reader:
            if row[2] in srcdict:
                srcdict[row[2]] += 1
            else:
                srcdict[row[2]] = 1
            if row[3] in srcdict:
                srcdict[row[3]] += 1
            else:
                srcdict[row[3]] = 1
            rowct += 1
            if rowct == topct:
                break

    ipct = 1
    for ip in srcdict:
        rowct=1
        if srcdict[ip] > 1:
            dstdict = {}
            with open(scores_full_path, 'rb') as f:
                reader = csv.reader(f, delimiter=',')
                reader.next()
                for row in reader:
                    if ip == row[2]:
                        dstdict[row[3]]=row[3]
                    if ip == row[3]:
                        dstdict[row[2]]=row[2]
                    rowct += 1
                    if rowct == topct:
                        break

            if len(dstdict.keys()) > 1:
                dstip = "'%s',"*len(dstdict.keys()) % tuple(dstdict.keys())
                hivestr = (" \"set hive.cli.print.header=true; SELECT sip as srcip," +
                "dip as dstip, MAX(ibyt) as maxbyte, AVG(ibyt) as avgbyte,  MAX(ipkt) as maxpkt, AVG(ipkt) as avgpkt " +
                " from " + "os.environ['DBNAME']" + "." + os.environ['DSOURCE']  +  
                " WHERE m="+mh+" AND d="+dy+" AND ( (sip=\'" + ip + "\' AND dip IN("+ dstip[:-1] + ")"
                " OR sip IN(" + dstip[:-1] + ") AND dip=\'" + ip + "\') ) " +
                " GROUP BY sip,dip \"  > " + spath + "chord-" + ip.replace(".","_") + ".tsv")
                disp.clear_output()
                print 'processing line ',ipct
                print hivestr
                #call(["hive", "-S", "-e", hivestr])
                check_output("hive -S -e " + hivestr, shell=True)
        if ipct == topct:
            break
        ipct += 1

main()
