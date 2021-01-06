import awswrangler as wr
import boto3
import os

bucket_name = 'com.brianpfeil.aws-data-wrangler'
csv_object_path = f's3://{bucket_name}/us-mortgage-companies.csv'


print(f'{csv_object_path} exists {wr.s3.does_object_exist(csv_object_path)}\n\n')

# read csv to pandas DataFrame
df = wr.s3.read_csv(csv_object_path)

print('first 3 rows of data\n\n', df.sample(n=3), '\n\n')

parquet_object_path = f's3://{bucket_name}/us-mortgage-companies.parquet'
print(
    f'converting from csv to parquet and storing at "{parquet_object_path}"\n\n')
wr.s3.to_parquet(df, parquet_object_path)


# execute athena query and return as pandas DataFrame
query = 'SELECT * FROM "default"."cloudfront_logs_brianpfeil_com" limit 10;'
df = wr.athena.read_sql_query(query, 'default')

print(f'{query} -> \n\n', df, '\n\n')
