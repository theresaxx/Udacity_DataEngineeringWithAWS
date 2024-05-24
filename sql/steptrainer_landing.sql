CREATE EXTERNAL TABLE IF NOT EXISTS `project`.`steptrainer_landing` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int,)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-landing-zone/steptrainer/landing/'
TBLPROPERTIES (
  'classification'='json')
