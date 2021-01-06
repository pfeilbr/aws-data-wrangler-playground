import awswrangler as wr
import boto3

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
