from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow.utils.dates import days_ago
import boto3
import tomli

########################################################################################
# CONFIG
########################################################################################
CONFIG_FILE_BUCKET = "de300spring2024-billyin"
CONFIG_FILE_KEY = "config.toml"

# Read in the configuration file from S3
def read_config_from_s3(client) -> dict:
    try:
        # Fetch the file from S3
        print(f"Reading config file from S3: {CONFIG_FILE_BUCKET}/{CONFIG_FILE_KEY}")
        response = client.get_object(Bucket=CONFIG_FILE_BUCKET, Key=CONFIG_FILE_KEY)
        file_content = response['Body'].read().decode('utf-8')
        print("CONFIG FILE CONTENTS: ", file_content)
        config = tomli.loads(file_content)
        return config
    except Exception as e:
        print(f"Failed to read from S3: {str(e)}")
        return {}

########################################################################################
# GLOBAL VARIABLES
########################################################################################
# S3 Client
# For local testing

S3C = boto3.client('s3', region_name='us-east-2')

EMRC = boto3.client('emr', region_name='us-east-2',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token)

# Read the configuration file
CONFIG = read_config_from_s3(S3C)
print("CONFIG: ", CONFIG)
CLUSTER_ID = CONFIG['aws']['cluster_emr']
CLUSTER_ID_MERGE = CONFIG['aws']['cluster_emr_merge']

# Dag Configuration
DAG_ARGS = {
    'owner': 'billyin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

############################################
# Helper Functions
############################################
def s3_pp(filename):
    return f's3://de300spring2024-billyin/emr_related/{filename}'

def cprint(msg: str) -> None:
    """
    Function to print messages with a prefix
    """
    print("[CINFO]", msg)

############################################
# EMR Main Functions
############################################

def create_emr_cluster(**kwargs):
    response = EMRC.run_job_flow(
        Name='billyin-hw4-cluster',
        ReleaseLabel='emr-7.1.0',
        LogUri='s3://de300spring2024-billyin/emr_related/logs',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'de300_instance1',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-013b0c5bdd6fd3339',  # subnet ID'
        },
        BootstrapActions=[
            {
                'Name': 'Install requirements',
                'ScriptBootstrapAction': {
                    'Path': 's3://de300spring2024-billyin/emr_related/bootstrap_installs.sh',
                }
            }
        ],
        Applications=[{'Name': 'Spark'}],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
    )
    cluster_id = response.get('JobFlowId')
    if not cluster_id:
        raise ValueError("Failed to create EMR cluster.")
    
    # Push the cluster ID to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='cluster_id', value=cluster_id)
    
    cprint(f"Cluster created with ID: {cluster_id}")
    return cluster_id

def check_cluster_created(**kwargs):
    """
    Wait for the cluster to be created, then proceed to the next task
    """
    cluster_id = kwargs['cluster_id'] if 'cluster_id' in kwargs else kwargs['ti'].xcom_pull(task_ids=kwargs.get('prev_task', None))
    cprint(f"Checking cluster status for ID: {cluster_id}")
    if not cluster_id:
        raise ValueError("No cluster ID available. Check previous task for errors.")
    waiter = EMRC.get_waiter('cluster_running')
    waiter.wait(ClusterId=cluster_id)
    return cluster_id

def check_cluster_already_created(**kwargs):
    """
    Check if the cluster already exists. If it does, proceed to the next task. If not, create the cluster.
    """
    cluster_id = kwargs.get('cluster_id', None)
    next_step = kwargs.get('next_step', None)
    nn_step = kwargs.get('nn_step', None)
    does_not_exist_step = kwargs.get('does_not_exist_step', None)

    def cluster_exists(cluster_id):
        # Check if the cluster exists on EMR using describe cluster
        try:
            response = EMRC.describe_cluster(ClusterId=cluster_id)
            status = response['Cluster']['Status']['State']
            
            # Check if the cluster is in a running or waiting state
            if status in ['RUNNING', 'WAITING']:
                return True
            else:
                cprint(f"Cluster is in {status} state.")
                return False
        except EMRC.exceptions.ClusterNotFound:
            cprint("Cluster not found.")
            return False
        except Exception as e:
            cprint(f"Error checking cluster status: {str(e)}")
            return False

    if cluster_id and cluster_exists(cluster_id):  # You need to define `cluster_exists` to check cluster status
        cprint(f"Cluster {cluster_id} already exists.")
        return [next_step, nn_step]  # Task to proceed if cluster exists
    else:
        cprint(f"Cluster {cluster_id} does not exist.")
        return [does_not_exist_step]  # Task to create cluster if not exists

def make_config(filename):
    return [
        {
            'Name': 'Run Python Script',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    filename,  # Specify the path to your Python script
                ]
            }
        }]
    
def add_step_to_cluster(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids=kwargs.get('task_name', 'check_cluster_created'))

    # Configuration for adding a Python script to the cluster
    step_config = make_config(kwargs['python_file'])

    # Add the new steps to the cluster
    response = EMRC.add_job_flow_steps(JobFlowId=cluster_id, Steps=step_config)
    step_ids = response['StepIds']

    # Optionally push the new step IDs to XCom for other tasks
    ti.xcom_push(key='new_step_ids', value=step_ids)

    return step_ids

def wait_step(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids=kwargs.get('task_name', 'check_cluster_created'))
    # If there are pervious steps, then wait for the previous step to complete
    # otherwise, proceed to the next step
    if kwargs.get('prev_add_step', None):
        step_ids = ti.xcom_pull(task_ids=kwargs.get('prev_add_step', None), key='new_step_ids')
        cprint(f"Cluster ID: {cluster_id}, Previous Step IDs: {step_ids}")
        # Wait until the previous step is completed
        waiter = EMRC.get_waiter('step_complete')
        try:
            # The StepId must be passed as a list even if it's a single StepId
            # Skip if there is no last_step_id
            if step_ids:
                for step_id in step_ids:
                    waiter.wait(ClusterId=cluster_id, StepId=step_id)
        except Exception as e:
            cprint(f"Error waiting for step completion: {str(e)}")
            raise   

############################################
# DAG Related Operations
############################################
dag = DAG(
    'emr_cluster_with_steps',
    default_args=DAG_ARGS,
    description='Create EMR Cluster and add steps using Boto3 within Airflow',
    schedule_interval='@daily',
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

############################################ P2 Tasks ############################################
create_emr_cluster_p2_task = PythonOperator(
    task_id='create_emr_cluster_p2',
    python_callable=create_emr_cluster,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

check_cluster_already_created_p2_task = BranchPythonOperator(
    task_id='check_cluster_already_created_p2',
    python_callable=check_cluster_already_created,
    provide_context=True,
    op_kwargs={'cluster_id': CLUSTER_ID, 'next_step': 'check_cluster_created_p2', 'nn_step': 'add_p2_1_to_cluster', 'does_not_exist_step': 'create_emr_cluster_p2'},
    dag=dag,
)

check_cluster_created_p2_task = PythonOperator(
    task_id='check_cluster_created_p2',
    python_callable=check_cluster_created,
    op_kwargs={'prev_task': 'create_emr_cluster_p2', 'cluster_id': CLUSTER_ID},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

p2_step_1 = PythonOperator(
    task_id='add_p2_1_to_cluster',
    python_callable=add_step_to_cluster,
    op_kwargs={'python_file': s3_pp(CONFIG['aws']['p2_1']), 'task_name': 'check_cluster_created_p2'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

p2_step_2 = PythonOperator(
    task_id='add_p2_2_to_cluster',
    python_callable=add_step_to_cluster,
    op_kwargs={'python_file': s3_pp(CONFIG['aws']['p2_2']), 'task_name': 'check_cluster_created_p2', 'prev_add_step':"add_p2_1_to_cluster"},
    provide_context=True,
    dag=dag,
)

pre_steps_check = PythonOperator(
    task_id='wait_for_any_before',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_p2'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

p2_step_1_check_task = PythonOperator(
    task_id='wait_for_p2_1',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_p2', 'prev_add_step': 'add_p2_1_to_cluster'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

p2_step_2_check_task = PythonOperator(
    task_id='wait_for_p2_2',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_p2', 'prev_add_step': 'add_p2_2_to_cluster'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

############################################ Merge Tasks ############################################
create_emr_cluster_merge_task = PythonOperator(
    task_id='create_emr_cluster_merge',
    python_callable=create_emr_cluster,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    provide_context=True,
    dag=dag,
)

check_cluster_already_created_merge_task = BranchPythonOperator(
    task_id='check_cluster_already_created_merge',
    python_callable=check_cluster_already_created,
    provide_context=True,
    op_kwargs={'cluster_id': CLUSTER_ID_MERGE, 'next_step': 'check_cluster_created_merge', 'nn_step': 'merge_1', 'does_not_exist_step': 'create_emr_cluster_merge'},
    dag=dag,
)

check_cluster_created_merge_task = PythonOperator(
    task_id='check_cluster_created_merge',
    python_callable=check_cluster_created,
    op_kwargs={'prev_task': 'create_emr_cluster_merge', 'cluster_id': CLUSTER_ID_MERGE},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

pre_merge_steps_check = PythonOperator(
    task_id='wait_for_any_before_merge',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_merge'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

merge_1 = PythonOperator(
    task_id='merge_1',
    python_callable=add_step_to_cluster,
    op_kwargs={'python_file': s3_pp(CONFIG['aws']['merge_1']), 'task_name': 'check_cluster_created_merge'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

merge_2 = PythonOperator(
    task_id='merge_2',
    python_callable=add_step_to_cluster,
    op_kwargs={'python_file': s3_pp(CONFIG['aws']['merge_2']), 'task_name': 'check_cluster_created_merge', 'prev_add_step':"merge_1"},
    provide_context=True,
    dag=dag,
)

check_step_completed_merge_1_task = PythonOperator(
    task_id='wait_for_merge_1',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_merge', 'prev_add_step': 'merge_1'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

check_step_completed_merge_2_task = PythonOperator(
    task_id='wait_for_merge_2',
    python_callable=wait_step,
    op_kwargs={'task_name': 'check_cluster_created_merge', 'prev_add_step': 'merge_2'},
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)


############################################ P2 Tasks ############################################
start_task >> check_cluster_already_created_p2_task >> [create_emr_cluster_p2_task, check_cluster_created_p2_task]
create_emr_cluster_p2_task >> check_cluster_created_p2_task
check_cluster_created_p2_task >> pre_steps_check >> p2_step_1 >> p2_step_1_check_task >> p2_step_2 >> p2_step_2_check_task

############################################ Merge Tasks ############################################
start_task >> check_cluster_already_created_merge_task >> [create_emr_cluster_merge_task, check_cluster_created_merge_task]
create_emr_cluster_merge_task >> check_cluster_created_merge_task
check_cluster_created_merge_task >> pre_merge_steps_check >> merge_1 >> check_step_completed_merge_1_task >> merge_2 >> check_step_completed_merge_2_task