import mysql.connector
import pytest
import pandas as pd
import numpy as np
import os

# Loading the environmental variables used for MYSQL database connection
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_DATABASE = os.environ.get('DB_DATABASE')

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
