#!/bin/bash

while getopts ":s:e:i:o:l:" opt; do
    case $opt in
        s) t_start="$OPTARG"
            ;;
        e) t_end="$OPTARG"
            ;;
        \?) echo "Invalid option -$OPTARG" >&2
            ;;
    esac
done
LIM=3000
ndate=$1

source /etc/duxbay.conf
export DBNAME
export DSOURCE
export LUSER

echo "executing ops for date: $ndate"
mv ${DSOURCE}_results.csv ${DSOURCE}_incr_results.csv
echo `python27 lda_ranking.py --date $ndate --user ${LUSER} --ifile ${DSOURCE}_incr_results.csv --ofile ${DSOURCE}_incr_scores.csv --limit ${LIM}`
echo `python2.7 add_nc_and_rep_services.py --date $ndate --user ${LUSER} --${DSOURCE}`
echo `python2.7 suspicious_connects_op.py --date $ndate --user ${LUSER} --${DSOURCE}`
cat ${DSOURCE}_incr_scores.csv >> ${DSOURCE}_scores.csv
python noise_filter.py --date $ndate --user ${LUSER} --file ${DSOURCE}_scores.csv --flow
rm -f ${DSOURCE}_incr_scores.csv


