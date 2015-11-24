#!/bin/bash

NFTABLE=netflow
QTILES='qtiles.tsv'
DSOURCE='flow'
source /etc/duxbay.conf


echo "generating byte deciles ..."
hive -e "set mapred.max.split.size=1073741824; set hive.exec.reducers.max=10;SELECT max(ibyt),qtile FROM (select ibyt, ntile(10) over (order by ibyt) AS qtile from ${NFTABLE} TABLESAMPLE(100 ROWS) ) q GROUP BY qtile ORDER BY qtile;" > ${QTILES}

echo "generating packet quintiles ..."
hive  -e "set mapred.max.split.size=1073741824; set hive.exec.reducers.max=10;SELECT max(ipkt),qtile FROM (select ipkt, ntile(3) over (order by ipkt) AS qtile from ${NFTABLE} TABLESAMPLE(100 ROWS) ) q GROUP BY qtile ORDER BY qtile;" >> ${QTILES}

echo "generating time deciles ..."
hive  -e "set mapred.max.split.size=1073741824; set hive.exec.reducers.max=10;SELECT max(dectime),qtile FROM (select (trhour + trminute/60.0) AS dectime, ntile(10) over (order by trhour,trminute) AS qtile from ${NFTABLE} TABLESAMPLE(100 ROWS) ) q GROUP BY qtile ORDER BY qtile;" >> ${QTILES}

python qtiles.py ${DSOURCE}

rm -rf qtiles.tsv
#mv qtiles.tsv qtiles

DSOURCE='dns'
#TODO