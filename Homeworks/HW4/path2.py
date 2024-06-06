from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine

# Read in the configuration file from S3
def read_config_from_s3() -> dict:
    try:
        # Fetch the file from S3
        response = S3C.get_object(Bucket=CONFIG_FILE_BUCKET, Key=CONFIG_FILE_KEY)
        file_content = response['Body'].read().decode('utf-8')
        config = tomli.loads(file_content)
        print("HELLO")
        print(config)
        return config
    except Exception as e:
        print(f"Failed to read from S3: {str(e)}")
        return {}

############################### Global Variables ###############################
# Params
PARAMS = read_config_from_s3()
# S3 Client
# For local testing
S3C = boto3.client('s3',
                        aws_access_key_id=PARAMS['local_dev']['aws_access_key_id'],
                        aws_secret_access_key=PARAMS['local_dev']['aws_secret_access_key'],
                        aws_session_token=PARAMS['local_dev']['aws_session_token'])
# On AWS
S3C = boto3.client('s3')
CONFIG_FILE_BUCKET = "de300spring2024-billyin"
CONFIG_FILE_KEY = "config.toml"
PARAMS = read_config_from_s3()

# Columns to keep
COLUMNS = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 
        'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']