SET hiveconf:data_location;
SET hiveconf:y;
SET hiveconf:m;
SET hiveconf:d;
SET hiveconf:h;
SET hiveconf:dbname;


DROP TABLE IF EXISTS ${hiveconf:dbname}.dns_tmp
;


CREATE EXTERNAL TABLE ${hiveconf:dbname}.dns_tmp (
  day STRING,
  timepart STRING,
  flen INT,
  sip STRING,
  dip STRING,
  rname STRING,
  rtype INT,
  rclass INT,
  flags INT,
  rcode INT,
  aname STRING
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:data_location}'
TBLPROPERTIES ('avro.schema.literal'='{
    "type":   "record"
  , "name":   "RawDnsRecord"
  , "namespace" : "com.cloudera.accelerators.dns.avro"
  , "fields": [
        {"name": "day",                        "type":["string",   "null"]}
      ,   {"name": "timepart",                        "type":["string",   "null"]}
     ,  {"name": "flen",                  "type":["int",   "null"]}
     ,  {"name": "sip",                 "type":["string",     "null"]}
     ,  {"name": "dip",                    "type":["string",   "null"]}
     ,  {"name": "rname",                    "type":["string",   "null"]}
     ,  {"name": "rtype",                     "type":["int",     "null"]}
     ,  {"name": "rclass",                       "type":["int",     "null"]}
     ,  {"name": "flags",                       "type":["int",     "null"]}
     ,  {"name": "rcode",              "type":["int",   "null"]}
     ,  {"name": "aname",             "type":["string",   "null"]}
  ]
}')
;


INSERT INTO TABLE ${hiveconf:dbname}.dns
PARTITION (date_part_key=${hiveconf:date_part_key})
SELECT   CONCAT(day + time) as treceived, unix_timestamp(CONCAT(day + time)) AS unix_tstamp, flen,  sip,  dip,  rname,  rtype,  rclass,  flags,  rcode,  aname
 FROM ${hiveconf:dbname}.dns_tmp
;
