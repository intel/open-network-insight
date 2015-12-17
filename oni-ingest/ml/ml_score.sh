#!/bin/bash

# read in variables (except for date) from etc/.conf file

FDATE=$1

DSOURCE=$3
TOL=$4

source /etc/duxbay.conf
export HPATH
export LUSER
export TOL
export KRB_AUTH

DPATH=$2

# intermediate ML results go in hive directory
DFOLDER='hive'


#kinit -kt /etc/security/keytabs/smokeuser.headless.keytab <user-id>
time spark-shell --master yarn-client --executor-memory  ${SPK_EXEC_MEM}  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g"  -i scala ${DSOURCE}_post_lda.scala

hadoop fs -copyToLocal ${HPATH}/scored/part-* ${LPATH}/.

cd ${LPATH}

cat part-* > ${DSOURCE}_results.csv
rm -f part-*

#op ml stage         Ingest results_all_20150618.csv into suspicious connects front end
source /etc/duxbay.conf

 #scp to UI node
 scp -r ${LPATH} ${UINODE}:${RPATH}
 

