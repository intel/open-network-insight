import shutil
import numpy as np
import linecache, bisect
import csv
import json
import operator
import sys, getopt, os

def flow_attack_heuristics(sconnect, srcdict, dstdict, sportdict, dportdict, flow_config):
    src_ip_index = flow_config['lda_file_schema']['srcIP']
    dst_ip_index = flow_config['lda_file_schema']['dstIP']
    sport_index = flow_config['lda_file_schema']['sport']
    dport_index = flow_config['lda_file_schema']['dport']
    headers = []
    external_ips = {}
    external_domains = {}    
    with open(sconnect, 'rb') as f:
        reader = csv.reader(f,delimiter=',') 
        headers = reader.next();        
        rowct = 1
        for row in reader:
            if row[src_ip_index] not in external_ips and row[headers.index('srcIpInternal')] == '0':
                external_ips[row[src_ip_index]] = 1
            elif row[src_ip_index] in external_ips:
                external_ips[row[src_ip_index]] += 1            
            if row[dst_ip_index] not in external_ips and row[headers.index('destIpInternal')] == '0':
                external_ips[row[dst_ip_index]] = 1
            elif row[dst_ip_index] in external_ips:
                external_ips[row[dst_ip_index]] += 1

            if row[headers.index('srcDomain')] not in external_domains and row[headers.index('srcIpInternal')] == '0':
                external_domains[row[headers.index('srcDomain')]] = 1
            elif row[headers.index('srcDomain')] in external_domains:
                external_domains[row[headers.index('srcDomain')]] += 1

            if row[headers.index('dstDomain')] not in external_domains and row[headers.index('destIpInternal')] == '0':
                external_domains[row[headers.index('dstDomain')]] = 1
            elif row[headers.index('dstDomain')] in external_domains:
                external_domains[row[headers.index('dstDomain')]] += 1

            if row[sport_index] not in sportdict:
                sportdict[row[sport_index]] = row[sport_index]
            if row[dport_index] not in dportdict:
                dportdict[row[dport_index]] = row[dport_index]
    
    for ip in external_ips:
        if external_ips[ip] > 20:
            print ip,' connects:', external_ips[ip]
    print ''
    for domain in external_domains:
        if external_domains[domain] > 20:
            print domain, 'connects: ', external_domains[domain]

def flow_apply_rules(rops,rvals,risk,scores_tmp_path, scores_path):
    rules_hash = 0   
    headers = get_scores_headers(scores_path)
    rules_fhash = [headers.index('srcIP'), headers.index('dstIP'), headers.index('sport'), headers.index('dport'), headers.index('ipkt'), headers.index('ibyt')]
    rowct = 0
    for k in xrange(len(rvals)):
        if rvals[k] != '':
            rules_hash += 2**k

    with open(scores_tmp_path, 'wb') as tmp:
        csv_writer = csv.writer(tmp, delimiter=',')
        csv_writer.writerow(headers)

        with open(scores_path, 'rb') as scores_file:
            csv_reader = csv.reader(scores_file, delimiter=',')
            csv_reader.next()
            for row in csv_reader:
                result = 0
                for n in xrange(0, len(rules_fhash)):
                    if(2**n & rules_hash) > 0:
                        if rops[n] == 'leq':
                            if int(row[rules_fhash[n]]) <= int(rvals[n]):
                                result += 2**n
                            if rops[n] == 'eq':
                                if int(row[rules_fhash[n]]) == int(rvals[n]):
                                    result += 2**n
                if result == rules_hash:
                    row[0] = risk
                    rowct += 1
                csv_writer.writerow(row)
    
    print 'Rows applied', rowct
    shutil.copyfile(scores_tmp_path, scores_path)

def flow_set_rules(scoring_rules, scores_tmp_path, scores_path):
    for rule in scoring_rules:
        flow_apply_rules(rule['rops'], rule['rvals'], rule['risk'], scores_tmp_path, scores_path)

def get_scores_headers(scores_path):
    headers = []
    with open(scores_path, 'rb') as f:
        reader = csv.reader(f,delimiter=',') 
        headers = reader.next();

    return headers

def main():
    flow = False
    dns = False
    scores_path = '{0}/ipython/user/{1}/{2}'
    scores_bu_path = scores_path
    scores_tmp_path = scores_path
    scores_new_path = scores_path
    ldadate = ''
    user_dir = ''
    dns_config = None
    flow_config = None
    lda_file_name= 'flow_scores.csv'

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:u:f', ["help", "date=", "user=","file=", 'flow', 'dns'])
    except getopt.GetoptError as err:
        print 'usage example: python noise_filter.py -d 20150412 -u duxbury [-f <file.csv>] [--flow OR --dns]'
        print 'OR'
        print 'usage example: python noise_filter.py --date 20150412 --user duxbury [--file <file.csv>] [--flow OR --dns]'
        sys.exit(2)

    if '--flow' in opts and '--dns' in opts:
        print 'Error: detected --flow and --dns in same command execution, please choose just one option.'        
        sys.exit(2)

    for opt,arg in opts:
        if opt in ('-h', "--help"):
            print 'usage example: python noise_filter.py -d 20150412 -u duxbury [-f <file.csv>] [--flow OR --dns]'
            print 'OR'
            print 'usage example: python noise_filter.py --date 20150412 --user duxbury [--file <file.csv>] [--flow OR --dns]'
            sys.exit() 
        elif opt in ('-d', '--date'):
            ldadate = arg
        elif opt in ('-u', '--user'):
            user_dir = arg
        elif opt in ('-f', '--file'):
            lda_file_name = arg
        elif opt == '--flow':
            flow = True
        elif opt == '--dns':
            dns = True

    if flow:
        
        if not os.path.isfile('./ldaFlowConfig.json'):
            print 'ldaFlowConfig.json file not found. Please make sure you have that file at the same path this script.'
            sys.exit(2)
        else:
            flow_config = json.load(open('./ldaFlowConfig.json'))

        scores_path = scores_path.format(user_dir, ldadate, lda_file_name)
        scores_bu_path = scores_bu_path.format(user_dir, ldadate, lda_file_name.replace('.csv', '_bu.csv'))
        scores_tmp_path = scores_tmp_path.format(user_dir, ldadate, lda_file_name + '.tmp')
        scores_new_path = scores_new_path.format(user_dir, ldadate, lda_file_name + '.new')
        srcdict = {}
        dstdict = {}
        sportdict = {}
        dportdict = {}
        
        print 'Setting Rules...'
        flow_set_rules(flow_config['scoring_rules'], scores_tmp_path, scores_path)

        print 'Showing Attack Heuristics:\n'
        flow_attack_heuristics(scores_path, srcdict, dstdict, sportdict, dportdict, flow_config)


#Execute main function
main()



