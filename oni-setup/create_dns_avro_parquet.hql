SET hiveconf:huser;
SET hiveconf:dbname;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:dbname}.dns (
  treceived STRING,
  unix_tstamp BIGINT,
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
PARTITIONED BY (y INT, m INT, d INT, h int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION '/user/${hiveconf:huser}/dns/hive'
TBLPROPERTIES ('avro.schema.literal'='{
    "type":   "record"
  , "name":   "DnsRecord"
  , "namespace" : "com.cloudera.accelerators.dns.avro"
  , "fields": [
        {"name": "treceived",                        "type":["string",   "null"]}
     ,  {"name": "unix_tstamp",                        "type":["long",   "null"]}
     ,  {"name": "flen",                  "type":["int",   "null"]}
     ,  {"name": "sip",                 "type":["string",     "null"]}
     ,  {"name": "dip",                    "type":["string",   "null"]}
     ,  {"name": "rname",                    "type":["string",   "null"]}
     ,  {"name": "rtype",                     "type":["int",     "null"]}
     ,  {"name": "rclass,                       "type":["int",     "null"]}
     ,  {"name": "flags",                       "type":["int",     "null"]}
     ,  {"name": "rcode",              "type":["int",   "null"]}
     ,  {"name": "aname",             "type":["string",   "null"]}
  ]
}');
