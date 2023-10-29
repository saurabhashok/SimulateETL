import boto3  # pip install boto3

# Let's use Amazon S3
s3 = boto3.client('s3', region_name='ap-south-1',aws_access_key_id='AKIA5HY4OROZKOR66G4I',
    aws_secret_access_key='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')

# response = s3.list_buckets()
#
# # Output the bucket names
# print('Existing buckets:')
# for bucket in response['Buckets']:
#     print(f'  {bucket["Name"]}')
bucket_name = 'internex-storage-bucket'
region = 'ap-south-1'

try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': region}
    )
    print(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
except Exception as e:
    print(f"Failed to create the bucket: {e}")
