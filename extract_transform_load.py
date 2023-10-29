import mysql.connector
import redshift_connector
import pytest
import pandas as pd
import numpy as np
import boto3
import os
import logging
from botocore.exceptions import ClientError

# Loading the environmental variables used for MYSQL database connection
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_DATABASE = os.environ.get('DB_DATABASE')

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

REDSHIFT_HOST = os.environ.get('REDSHIFT_HOST')
REDSHIFT_PORT = os.environ.get('REDSHIFT_PORT')
REDSHIFT_USER = os.environ.get('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.environ.get('REDSHIFT_PASSWORD')
REDSHIFT_DATABASE = os.environ.get('REDSHIFT_DATABASE')


with mysql.connector.connect(host=DB_HOST, database=DB_DATABASE, user=DB_USERNAME, password=DB_PASSWORD) as conn, open('exported.dat', 'w') as output:
    cursor = conn.cursor()
    sql_query = """
    SELECT * FROM my_data
    WHERE flag = true
    """
    cursor.execute(sql_query)
    data = cursor.fetchall()
    for row in data:
        output.write('\t'.join(map(str, row)) + '\n')

with open('exported.dat', 'r') as exported_file, open('transformed.dat', 'w') as transformed_file:
    for row in exported_file:
        line = row.strip().split('\t')
        if len(line) == 4:
            id, StringA, StringB, flag = line
            transformed_string = StringB.upper() + '_'
            transformed_file.write(f"{id}\t{transformed_string}\t{StringB}\t{flag}")


#Create a s3 bucket in AWS
def create_bucket(bucket_name, region=None):

    global s3_client
    s3_client = boto3.client('s3', region_name=region)
    location = {'LocationConstraint':region}

    bucket_exists = False
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        bucket_exists = True
    except Exception as e:
        if "404" in str(e):
            bucket_exists = False
        else:
            print(f"Error checking bucket existence:{e}")

    if not bucket_exists:
        try:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            print(f"S3 bucket {bucket_name} is created successfully")
        except Exception as e:
            print(f"failed to create the bucket: {e}")
    else:
        print(f"The bucket {bucket_name} already exists in AWS S3")

aws_bucket_name = 'internex-data-storage'
aws_region = 'ap-south-1'

create_bucket(aws_bucket_name, aws_region)


def upload_file_s3(object_key, file_path, aws_bucket_name):
    file_exists = False
    try:
        s3_client.head_object(Bucket=aws_bucket_name, key=object_key)
        file_exists = True
        print(f"The object {object_key} exists in the database {aws_bucket_name}")
    except Exception as e:
        if "404" in str(e):
            bucket_exists = False
        else:
            print(f"Error checking bucket existence:{e}")

    if not file_exists:
        s3_client.upload_file(file_path, aws_bucket_name, object_key)
        print(f"{file_path} uploaded to {aws_bucket_name}/{object_key}")
    else:
        print(f"The file {object_key} already exists in {aws_bucket_name}")

object_key = 'transformed.dat'
file_path = 'transformed.dat'

upload_file_s3(object_key, file_path, aws_bucket_name)

s3_uri = 's3://hosted-data-bucket/exported.dat'

with redshift_connector.connect(host=REDSHIFT_HOST, database=REDSHIFT_DATABASE, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD) as redshift_conn:
    cursor = redshift_conn.cursor()
    table_name = "internex_data"

    create_redshift_table = f"""
    CREATE TABLE IF NOT EXISTS table_name(
                 id INT PRIMARY KEY,
                 StringA varchar(20),
                 StringB varchar(20),
                flag BOOLEAN
    )
    """
    cursor.execute(create_redshift_table)

    copy_data_redshift = """
                   COPY data
				   FROM {s3_uri}
				   CREDENTIALS 'aws_access_key_id={aws_access_key};
				   aws_secret_access_key={aws_secret_key}'
				   DELIMTER '\t';
			        """
    cursor.execute(copy_data_redshift)

@pytest.fixture
def df():
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    return df

def test_unique(df):
    assert df["id"].is_unique, "There are duplicates detected in the column"

def test_col_exists(df):
    name = "id"
    assert name in df.columns, "The column does not exist"

def test_is_null_exists(df):
    assert np.where(df['StringB'].isnull()), "There are count(df['StringB']).isnull() null values"

def test_col_datatype(df):
    assert (df['StringA'].dtype == np.str_ ), "There are values other than string datatype"

def test_col_flag_check(df):
    assert set(df.flag.unique()) == {1, 0}, 'flag has other values'

test_path = '/Users/saurabh/Documents/Elastic/test.py'
pytest.main([test_path])
