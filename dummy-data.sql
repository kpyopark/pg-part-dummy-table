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

-- date -d '2021-10-01 +1 month' +'%Y-%m-%d'

create table tb_part_dummy_201901 partition of tb_part_dummy for values from ('2019-01-01') to ('2019-02-01');
