import awswrangler as wr
import boto3

bucket_name = 'sam-deploy-bucket-01'
object_path = f's3://{bucket_name}/0da10f95b94d5ea8d3bf9ee52ddecb2c.template'

print(f'{object_path} exists {wr.s3.does_object_exist(object_path)}')