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

-- Preprocessing to upload result tables into Aurora

CREATE TABLE tb_part_dummy_tsv (
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
LOCATION 's3://apg-export-test/tb_part_dummy_tsv/'


insert into tb_part_dummy_tsv (
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
from tb_part_dummy
;

-- The above SQL makes the below tsv.gz files in the target directory.
--
-- 20220105_042303_00034_t7ccs_08337c42-17e0-4a87-8234-8815abdde29f.gz	gz	January 5, 2022, 13:23:26 (UTC+09:00)	718.2 MB
-- 20220105_042303_00034_t7ccs_0992bd57-2c20-4af9-b654-b56e45449c82.gz	gz	January 5, 2022, 13:23:27 (UTC+09:00)	686.7 MB
-- 20220105_042303_00034_t7ccs_0c9d40ea-7f9f-4801-a48e-9f3fddf26dd9.gz	gz	January 5, 2022, 13:23:26 (UTC+09:00)	695.2 MB
-- ...

