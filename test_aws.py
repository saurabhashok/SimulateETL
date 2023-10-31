import boto3  # pip install boto3
import psycopg
import redshift_connector



response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')
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


with psycopg.connect(host=REDSHIFT_HOST,
                      database=REDSHIFT_DATABASE,
                      user=REDSHIFT_USER,
                      password=REDSHIFT_PASSWORD) as redshift_conn:
    cur = redshift_conn.cursor()
    cur.execute("""SELECT * FROM "dev"."public"."category";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)


try:
    with redshift_connector.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT, database=REDSHIFT_DATABASE, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD) as redshift_conn:
        # Get a cursor
        cursor = redshift_conn.cursor()

        # Define the table name
        table_name = 'test_table'

        # SQL statement to create the table
        create_redshift_table = f"""
        CREATE TABLE IF NOT EXISTS data.{table_name}(
                     id INT PRIMARY KEY,
                     StringA varchar(20),
                     StringB varchar(20),
                     StringC varchar(20),
                     flag BOOLEAN
        )
        """
        cursor.execute(create_redshift_table)
        redshift_conn.commit()
except Exception as e:
    print(f"Error creating Redshift table: {e}")
