#!/bin/sh

# Prerequisite
# 1. Aurora Database has communication channel with AWS S3 service via NAT Gateway.
# 2. Security group assiged to the Aurora cluster should have outbound access permision for 443/80 port to the internet.

psql <<EOF
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
EOF

bucket_name=apg-export-test
cluster_id=g-lsn-tets
table_name=tb_part_dummy
region=ap-northeast-2

aws s3 mb s3://${bucket_name}

aws iam create-policy --policy-name rds-s3-export-policy  --policy-document '{
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "s3export",
         "Action": [
           "S3:PutObject",
           "S3:AbortMultipartUpload"
         ],
         "Effect": "Allow",
         "Resource": [
           "arn:aws:s3:::${bucket_name}/*"
         ] 
       }
     ] 
   }'

aws iam create-role  --role-name rds-s3-export-role  --assume-role-policy-document '{
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
            "Service": "rds.amazonaws.com"
          },
         "Action": "sts:AssumeRole"
       }
     ] 
   }'

policy_arn=`aws iam list-policies | jq -r '.Policies[] | select(.PolicyName == "rds-s3-export-policy").Arn'`

aws iam attach-role-policy  --policy-arn ${policy_arn} --role-name rds-s3-export-role  

role_arn=`aws iam list-roles | jq -r '.Roles[] | select(.RoleName == "rds-s3-export-role").Arn'`

aws rds add-role-to-db-cluster \
   --db-cluster-identifier ${cluster_id} \
   --feature-name s3Export \
   --role-arn ${role_arn}   \
   --region ${region}

psql <<EOF
select current_timestamp;
SELECT * from aws_s3.query_export_to_s3('select * from ${table_name}', 
   aws_commons.create_s3_uri('${bucket_name}', '${table_name}_org/contents', '${region}') 
);
select current_timestamp;
EOF

