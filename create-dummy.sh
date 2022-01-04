#!/bin/sh

psql <<EOF
create table tb_part_dummy (
  logdate date not null, 
  logid   bigserial,
  col_1_f float, 
  col_2_f float,
  col_3_f float,
  col_4_f float,
  col_5_f float,
  col_6_v varchar(255),
  col_7_v varchar(255),
  col_8_v varchar(255),
  col_9_d double precision,
  col_10_d double precision,
  col_11_m decimal(25,5),
  col_12_m decimal(25,5)
  constraint pk_tb_part_dummy (logdate, logid)
) partition by range(logdate);
EOF

# 

cur_yearmonth=`date +'%Y-%m'`
for past_month in {0}
do
  startymd=`date -d '${cur_yearmonth}-01 -${past_month} month' +'%Y-%m-%d'`
  startym=`date -d '${startymd}' +'%Y-%m'`
  nextymd=`date -d '${startymd} +1 month' +'%Y-%m-%d'`
  psql <<EOF
create table tb_part_dummy_${startym} partition of tb_part_dummy for values from (\'${startymd}\') to (\'${nextymd}\');
EOF
done


