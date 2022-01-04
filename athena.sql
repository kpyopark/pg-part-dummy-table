CREATE TABLE tb_part_dummy_org (
  logdate date, 
  logid   bigint,
  col_1_f float, 
  col_2_f float,
  col_3_f float,
  col_4_f float,
  col_5_f float,
  col_6_v varchar(255),
  col_7_v varchar(255),
  col_8_v varchar(255),
  col_9_d double,
  col_10_d double,
  col_11_m decimal(25,5),
  col_12_m decimal(25,5)
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t'
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://apg-export-test/tb_part_dummy/'

CREATE TABLE tb_part_dummy (
  logdate date,
  logid   bigint,
  col_1_f float, 
  col_2_f float,
  col_3_f float,
  col_4_f float,
  col_5_f float,
  col_6_v varchar(255),
  col_7_v varchar(255),
  col_8_v varchar(255),
  col_9_d double,
  col_10_d double,
  col_11_m decimal(25,5),
  col_12_m decimal(25,5)
)
PARTITIONED BY (logym string)
STORED AS PARQUET
LOCATION 's3://apg-export-test/tb_part_dummy_refined/'
tblproperties ("parquet.compress"="SNAPPY")

insert into tb_part_dummy (
logym,
logdate, 
logid,
col_1_f, 
col_2_f,
col_3_f,
col_4_f,
col_5_f,
col_6_v,
col_7_v,
col_8_v,
col_9_d,
col_10_d,
col_11_m,
col_12_m
) SELECT
date_format(logdate, '%Y%m') as logym
logdate,
logid,
col_1_f, 
col_2_f,
col_3_f,
col_4_f,
col_5_f,
col_6_v,
col_7_v,
col_8_v,
col_9_d,
col_10_d,
col_11_m,
col_12_m
from tb_part_dummy_org
;
