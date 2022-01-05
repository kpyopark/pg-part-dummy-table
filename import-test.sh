#!/bin/sh

bucket_name=apg-export-test
cluster_id=pg-lsn-tets
table_name=tb_part_dummy
result_table_name=tb_part_dummy_tsv
region=ap-northeast-2
import_policy_name=rds-s3-import-policy
import_role_name=rds-s3-import-role

aws iam create-policy --policy-name ${import_policy_name}  --policy-document '{
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "s3export",
         "Action": [
           "S3:GetObject",
           "S3:ListMultipartUploadParts",
           "s3:ListBucket"
         ],
         "Effect": "Allow",
         "Resource": [
           "arn:aws:s3:::${bucket_name}/*"
         ] 
       }
     ] 
   }'

aws iam create-role  --role-name ${import_role_name} --assume-role-policy-document '{
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

policy_arn=`aws iam list-policies | jq -r '.Policies[] | select(.PolicyName == '\"${import_policy_name}\"').Arn'`

aws iam attach-role-policy  --policy-arn ${policy_arn} --role-name ${import_role_name}  

role_arn=`aws iam list-roles | jq -r '.Roles[] | select(.RoleName == '\"${import_role_name}\"').Arn'`

aws rds add-role-to-db-cluster \
    --db-cluster-identifier ${cluster_id} \
    --feature-name s3Import \
    --role-arn ${role_arn}

# After to make result tables with TSV(gz) format. we can load from the data from query_import_s3 function
 
# psql <<EOF
# select current_timestamp;
# SELECT * from aws_s3.table_import_from_s3('${result_table_name}',  '', '(format text)',
#    aws_commons.create_s3_uri('${bucket_name}', '${table_name}_tsv/20220105_042303_00034_t7ccs_08337c42-17e0-4a87-8234-8815abdde29f.gz', '${region}') 
# );
# select current_timestamp;
# EOF

# psql <<EOF
# select current_timestamp;
# SELECT * from aws_s3.table_import_from_s3('tb_part_dummy_tsv',  '', '(format text)',
#    aws_commons.create_s3_uri('apg-export-test', 'tb_part_dummy_tsv/20220105_042303_00034_t7ccs_08337c42-17e0-4a87-8234-8815abdde29f.gz', 'ap-northeast-2') 
# );
# select current_timestamp;
# EOF

# psql <<EOF
# select current_timestamp;
# SELECT * from aws_s3.table_import_from_s3('tb_part_dummy_tsv',  '', '(format text)',
#    'apg-export-test', $$20220105_042303_00034_t7ccs_08337c42-17e0-4a87-8234-8815abdde29f.gz$$, 'ap-northeast-2'
# );
# select current_timestamp;
# EOF

psql <<EOF
\copy ${result_table_name} from './<<downloaded file>>' with delimiter E'\t';
EOF

# https://stackoverflow.com/questions/6958965/how-to-copy-data-from-file-to-postgresql-using-jdbc

# import java.io.FileReader;
# import java.sql.Connection;
# import java.sql.DriverManager;

# import org.postgresql.copy.CopyManager;
# import org.postgresql.core.BaseConnection;

# public class PgSqlJdbcCopyStreamsExample {

#     public static void main(String[] args) throws Exception {

#         if(args.length!=4) {
#             System.out.println("Please specify database URL, user, password and file on the command line.");
#             System.out.println("Like this: jdbc:postgresql://localhost:5432/test test password file");
#         } else {

#             System.err.println("Loading driver");
#             Class.forName("org.postgresql.Driver");

#             System.err.println("Connecting to " + args[0]);
#             Connection con = DriverManager.getConnection(args[0],args[1],args[2]);

#             System.err.println("Copying text data rows from stdin");

#             CopyManager copyManager = new CopyManager((BaseConnection) con);

#             FileReader fileReader = new FileReader(args[3]);
#             copyManager.copyIn("COPY t FROM STDIN", fileReader );

#             System.err.println("Done.");
#         }
#     }
# }

