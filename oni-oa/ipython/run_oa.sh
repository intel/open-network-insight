#!/bin/bash

LIM=3000
ndate= $1

source /etc/duxbay.conf
EXPORT DBNAME
EXPORT DSOURCE
EXPORT LUSER
EXPORT LIM

echo "executing ops for date: $ndate"
echo `python27 lda_ranking.py --date $ndate --user ${LUSER} --ifile ${DSOURCE}_results.csv --ofile ${DSOURCE}_scores.csv --limit ${LIM}`
echo `python2.7 add_nc_and_rep_services.py --date $ndate --user ${LUSER} --${DSOURCE}`
echo `python2.7 suspicious_connects_op.py --date $ndate --user ${LUSER} --${DSOURCE}`


