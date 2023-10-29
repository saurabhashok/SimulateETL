import boto3  # pip install boto3

# Let's use Amazon S3


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
