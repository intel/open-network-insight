import shutil
import os
import sys ,getopt
import json

def write_new_rows(schema_config, outfile, rows):
    outfile.write(','.join(schema_config['lda_scores_file_columns']) + "\n")
                
    for i, row in enumerate(rows):
        row = row[0:len(row)-1]
        row_columns = row.split(',')
        new_row = '0' # new First column ('sev')

        for index in schema_config['column_indexes']:
            new_row += ',' + row_columns[index]

        new_row += ','+ row_columns[len(row_columns)-1] + ',' + str(i) + '\n'

        outfile.write(new_row)

def main():
    ldadate = ''
    sconnects_folder = '/{0}/ipython/user/{1}/'	
    limit = 3000
    ifile = ''
    scores_f = ''
    user = ''
    flow = False
    dns= False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:u:l:i:o:",["help", "date=", "user=", "limit=", "ifile=", "ofile=", "flow", "dns"])
    except getopt.GetoptError as err:
        print 'lda_ranking.py -d <date(yyyymmdd)> -u <userDir> -l <3000> -i <inputFile.csv> -o <outputFile.csv> [--flow OR --dns]'
        sys.exit(2)

    if '--flow' in opts and '--dns' in opts:
        print 'Error: detected --flow and --dns in same command execution, please choose just one option.'
        print 'Usage: lda_ranking.py -d <date(yyyymmdd)> -u <userDir> -l <3000> -i <inputFile.csv> -o <outputFile.csv> [--flow OR --dns]'
        sys.exit()

    for opt,arg in opts:
        if opt in ('-h', "--help"):
            print 'lda_ranking.py -d <date(yyyymmdd)> -u <userDir> -l <3000> -i <inputFile.csv> -o <outputFile.csv> [--flow OR --dns]'
            sys.exit()        
        elif opt in ("-d", "--date"):
            ldadate = arg
        elif opt in ("-l", "--limit"):
            limit = int(arg)
        elif opt in ("--ifile", "-i"):
            ifile = arg
        elif opt in ("--ofile", "-o"):
            scores_f = arg
        elif opt in ("-u", "--user" ):
            if arg[0] == '/':
                arg = arg[1:]
            user = arg
        elif opt == '--flow':
            flow = True
        elif opt == '--dns':
            dns = True            
    
    if flow == False and dns == False:
        print 'INFO: no --flow or --dns options detected. The script will use flow schema by default'
        flow = True

    sconnects_folder = sconnects_folder.format(user,ldadate)
    ifile = sconnects_folder + ifile
    scores_f = sconnects_folder + scores_f

    # create new folder for day ldadate
    if not os.path.exists(sconnects_folder):
        os.makedirs(sconnects_folder)

    # create edge_investigation notebook for the day
    edge_investigation_path =  user+'/ipython/master/Edge_Investigation_master.ipynb'
    threat_investigation_path = user+'/ipython/master/Threat_Investigation_master.ipynb'
    edge_investigation_day_path = sconnects_folder + '/Edge_Investigation_'+ldadate+'.ipynb'
    threat_investigation_day_path = sconnects_folder + '/Threat_Investigation_'+ldadate+'.ipynb'

    if not os.path.isfile(edge_investigation_day_path):
        shutil.copy(edge_investigation_path, edge_investigation_day_path)

    if not os.path.isfile(threat_investigation_day_path):
        shutil.copy(threat_investigation_path, threat_investigation_day_path)

    with open(ifile, 'rb') as infile:
        rows = [row for row in infile][:limit]
        with open(scores_f, 'wb') as outfile:
            if flow == True:
                flow_config = None
                if not os.path.isfile('./ldaFlowConfig.json'):
                    print 'ldaFlowConfig.json file not found. Please make sure you have that file at the same path this script.'
                else:
                    flow_config = json.load(open('./ldaFlowConfig.json'))

                if flow_config != None:
                    write_new_rows(flow_config, outfile, rows)                    

            elif dns == True:
                dns_config = None                
                if not os.path.isfile('./ldaDnsConfig.json'):
                    print 'ldaDnsConfig.json file not found. Please make sure you have that file at the same path this script.'
                else:
                    dns_config = json.load(open('./ldaDnsConfig.json'))

                if dns_config != None:
                    write_new_rows(dns_config, outfile, rows)                    
                
main()
