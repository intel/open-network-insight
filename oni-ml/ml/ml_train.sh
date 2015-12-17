#!/bin/bash

# read in variables (except for date) from etc/.conf file

FDATE=$1
YR=$2
MH=$3
DY=[$4-$5]
DSOURCE=$6
TOL=$7

echo "training for days $DY"
# intermediate ML results go in hive directory
DFOLDER='hive'

#  pre-processing scala

source /etc/duxbay.conf
export DPATH
export HPATH
export LUSER
export TOL
export KRB_AUTH

hadoop fs -rm -R ${HPATH}/word_counts
hadoop fs -rm -R ${HPATH}/lda_word_counts

CUT=$(<${DSOURCE}_qtiles)
export CUT

#kinit -kt /etc/security/keytabs/smokeuser.headless.keytab <user-id>
time spark-shell --master yarn-client --executor-memory  ${SPK_EXEC_MEM}  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g"  -i scala ${DSOURCE}_pre_lda.scala

# move TDM to local file system
# do we have to do a mkdir here?
rm -rf ${LPATH}
mkdir ${LPATH}

hadoop fs -copyToLocal  ${HPATH}/word_counts/part-* ${LPATH}/.
cd ${LPATH}
cat part-* > doc_wc.dat
rm -f part-*

#   lda  stage
source /etc/duxbay.conf
cd ..
time python lda_pre.py ${LPATH}/
rm -f ${LPATH}/doc_wc.dat
wc -l ${LPATH}/model.dat
sleep 2
#  copy input files to all nodes in a new folder for this day's LDA results
# use array scp 
for d in "${NODES[@]}" 
do 
	echo "copying $d ${LPATH} ${LUSER}"
	scp -r ${LPATH} $d:${LUSER}/ml/.    
done
sleep 2
cd ${LDAPATH}
time mpiexec -n 20 -f machinefile ./lda est 2.5 20 settings.txt ../${FDATE}/model.dat random ../${FDATE}
sleep 10

cd ${LUSER}/ml
time python lda_post.py ${LPATH}/


# post-processing stage
source /etc/duxbay.conf
export DPATH
export HPATH
export TOL
hadoop fs -rm ${HPATH}/doc_results.csv
hadoop fs -put ${LPATH}/doc_results.csv ${HPATH}/.
hadoop fs -rm ${HPATH}/word_results.csv
hadoop fs -put ${LPATH}/word_results.csv ${HPATH}/.