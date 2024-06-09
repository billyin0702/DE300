from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the Python functions for each task
def fetch_data():
    print("Fetching data...")
    # Simulate data fetching
    return {'data': [1, 2, 3, 4, 5]}

def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    print(f"Processing data: {data}")
    processed_data = [x * 2 for x in data['data']]
    return {'processed_data': processed_data}

def store_data(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_data')
    print(f"Storing data: {processed_data}")

# Define default arguments for the DAG, stop running by itself
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_data_processing',
    default_args=default_args,
    description='A simple DAG that fetches, processes, and stores data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define tasks using PythonOperator
task_fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

task_process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

task_store_data = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_fetch_data >> task_process_data >> task_store_data
