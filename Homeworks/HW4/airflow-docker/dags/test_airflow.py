# Test file for the DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
#from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import OperationalError


# Default arguments
default_args = {
    'owner': 'johndoe',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

def test_dag_structure():
    print("Testing the DAG structure")

def test_dag_structure_2():
    print("Testing the DAG structure")

def test_rds_conn():
    # db_alchemy_driver = "postgresql+psycopg2"
    # username = "pgde300"
    # password = "de300spring2024"
    # port = "5432"
    # endpoint = "billyin-hw4-pub-acc-2.cvwhaidrmrqj.us-east-2.rds.amazonaws.com"
    # default_db = "heart_disease_master"
    # CONN_URI = f"{db_alchemy_driver}://{username}:{password}@{endpoint}:{port}/{default_db}"

    # print("CONN_URI: ", CONN_URI)

    # # Create a connection to the RDS
    # engine = create_engine(CONN_URI)
    # conn = engine.connect()

    # print("Testing the RDS connection finished")

    # Connection parameters
    conn = None
    try:
        # Connection parameters
        conn = psycopg2.connect(
            host="billyin-hw4-pub-acc-3.cvwhaidrmrqj.us-east-2.rds.amazonaws.com",
            dbname="heart_disease_master",
            user="pgde300",
            password="de300spring2024",
            port="5432"
        )

        # Create a cursor object
        cur = conn.cursor()
        
        # Print PostgreSQL version
        cur.execute('SELECT version();')
        db_version = cur.fetchone()
        print("You are connected to - ", db_version)
        
        # Close the communication with the PostgreSQL
        cur.close()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

# Define the DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,
)

# Define the tasks
t1 = PythonOperator(
    task_id='test_dag_structure',
    python_callable=test_dag_structure,
    dag=dag,
)

t2 = PythonOperator(
    task_id='test_dag_structure_2',
    python_callable=test_dag_structure_2,
    dag=dag,
)

t3 = PythonOperator(
    task_id='test_rds_conn',
    python_callable=test_rds_conn,
    dag=dag,
)

t1 >> t2 >> t3