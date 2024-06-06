from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine

################################ Config Functions ################################
CONFIG_FILE_BUCKET = "de300spring2024-billyin"
CONFIG_FILE_KEY = "config.toml"

# Read in the configuration file from S3
def read_config_from_s3(client) -> dict:
    try:
        # Fetch the file from S3
        print(f"Reading config file from S3: {CONFIG_FILE_BUCKET}/{CONFIG_FILE_KEY}")
        response = client.get_object(Bucket=CONFIG_FILE_BUCKET, Key=CONFIG_FILE_KEY)
        file_content = response['Body'].read().decode('utf-8')
        config = tomli.loads(file_content)
        return config
    except Exception as e:
        print(f"Failed to read from S3: {str(e)}")
        return {}

############################### Global Variables ###############################
# S3 Client
# For local testing
S3C = boto3.client('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token)
# # On AWS
# S3C = boto3.client('s3')

# Params
PARAMS = read_config_from_s3(S3C)

# Columns, group together numerical, categorical, and binary feature column names
numerical_features = ['age', 'trestbps', 'chol', 'thalach', 'oldpeak', 'tpeakbps', 'trestbpd', 'tpeakbpd', 'thaldur', 'thalrest']
categorical_features = ['restecg']
binary_features = ['sex', 'htn', 'dig', 'prop', 'nitr', 'pro', 'exang', 'xhypo', 'dummy']

mode_features = numerical_features + categorical_features + binary_features

# Dag Configuration
DAG_ARGS = {
    'owner': 'billyin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# RDS
CONN_URI = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['endpoint']}:{PARAMS['db']['port']}/{PARAMS['db']['default_db']}"
print(f"RDS Connection URI: {CONN_URI}")

################################ Helper Functions ################################
def fetch_data_from_rds(func):
    """
    Decorator function to fetch the data from the RDS
    """
    def wrapper(*args, **kwargs):
        # Create a connection to the RDS
        engine = create_engine(CONN_URI)
        conn = engine.connect()

        # Fetch the data from the RDS tranform into a Pandas DataFrame
        data = pd.read_sql(f"SELECT * FROM {PARAMS['db']['table_name_p1']}", conn)

        # Call the function
        ret = func(data)

        # Close the connection
        conn.close()

        # Return the data
        return ret
    return wrapper

def initial_impute():
    """
    Function to impute the initial values for the missing data, similar to HW1

    Steps:
    1. Fetch data from S3
    2. Read the CSV
    3. Validity filtering
    """
    print("[CINFO] Initial imputation started")
    # Fetch csv from S3
    response = S3C.get_object(Bucket=PARAMS['s3']['bucket'], Key=PARAMS['s3']['data_key'])

    # Read the CSV
    data = pd.read_csv(response['Body'])

    print("[CINFO] Data read from S3 successfully")

    # Validity filtering
    # Function to check if any value in a row contains spaces or is non-numeric
    def is_invalid(row):
        for item in row:
            if isinstance(item, str) and (' ' in item or not item.replace('.', '', 1).isdigit()):
                return True
        return False
    
    # Filter out rows with invalid data
    data = data[~data.apply(is_invalid, axis=1)]

    # Remove columns if more than 10% of the values are missing
    threshold = len(data) * 0.90 # Number of non-na values
    data = data.dropna(thresh=threshold, axis=1)

    print("All columns in the 'heart_disease.csv' file:")
    print("Number of columns: ", len(data.columns))
    print(data.columns)

    # Convert all object type columns to float64
    for col in data.columns:
        if data[col].dtype == 'object':
            data[col] = data[col].astype('float64')

    # Check if the database table exists
    engine = create_engine(CONN_URI)
    conn = engine.connect()
    table_exists = engine.dialect.has_table(conn, PARAMS['db']['db_name'])

    # If the table exists, drop it
    if table_exists:
        engine.execute(f"DROP TABLE {PARAMS['db']['db_name_p1']}")
    else:
        print(f"Table '{PARAMS['db']['db_name_p1']}' does not exist")
        # Create the table
        data.head(0).to_sql(PARAMS['db']['db_name_p1'], engine, if_exists='replace', index=False)

    # Put data into RDS
    data.to_sql(PARAMS['db']['db_name_p1'], engine, if_exists='replace', index=False)

    # Log progress
    print("[CINFO] Initial imputation complete")

    # Return the data
    conn.close()
    return data

@fetch_data_from_rds
def second_impute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to impute the initial values for the missing data, similar to HW1
    """
    # Remove temporal features
    data.drop(columns=['ekgmo', 'ekgday(day', 'ekgyr', 'cmo', 'cday', 'cyr'], inplace=True)

    # Impute missing values using mode for remaining columns
    for column in mode_features:
        mode_value = data[column].mode().iloc[0]  # Get the mode and handle potential multiple modes
        print(f"Imputing mode value for column '{column}': {mode_value}")
        data[column].fillna(mode_value, inplace=True)
        
    # Remove any remaining rows with NA values
    data.dropna(inplace=True)
        
    # Re-evaluate number of rows and columns with at least one NA
    # Identifying columns with any NA values
    columns_with_na = data.columns[data.isna().any()].tolist()
    print(f"Columns containing NA values: {columns_with_na}")

    # Counting rows with at least one NA
    na_row_count = data.isna().any(axis=1).sum()
    print(f"Number of rows containing at least one NA value: {na_row_count}")

    # Return the cleaned data
    return data

@fetch_data_from_rds
def feature_engineering(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to perform feature engineering on the dataset
    """
    # Transform the categorical features into one-hot encoded columns
    data = pd.get_dummies(data, columns=categorical_features)

    print("Data after one-hot encoding of categorical features:")
    print(data.head())

    # Return the transformed data
    return data

@fetch_data_from_rds
def train_models(data: pd.DataFrame) -> None:
    """
    Function to train the model on the dataset, specifically SVM and Logistic Regression
    """
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
    from sklearn.linear_model import LogisticRegression
    from sklearn.svm import SVC

    # Split the data into training and testing sets
    X = data.drop(columns=['target'])
    y = data['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=PARAMS['ml']['test_ratio'], random_state=42)

    # Train the Logistic Regression model
    lr_model = LogisticRegression(max_iter=1000)
    lr_model.fit(X_train, y_train)
    lr_score = accuracy_score(y_test, lr_model.predict(X_test))
    print(f"Logistic Regression model accuracy: {lr_score}")

    # Train the SVM model
    svm_model = SVC()
    svm_model.fit(X_train, y_train)
    svm_score = accuracy_score(y_test, svm_model.predict(X_test))
    print(f"SVM model accuracy: {svm_score}")

    # Select the model with the highest accuracy
    model = lr_model if lr_score > svm_score else svm_model

    # Return the model
    return model

################################ DAG Configuration ################################
# Create the DAG
dag = DAG(
    'p1_dag',
    default_args=DAG_ARGS,
    description='DAG for Homework 4',
    schedule_interval=PARAMS['workflow']['workflow_schedule_interval'],
    tags=['de300', 'homework4'],
)

# Dummy Operator
dummy = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)

# Task 1: Initial imputation
initial_impute_task = PythonOperator(
    task_id='initial_impute',
    python_callable=initial_impute,
    op_args=[],
    dag=dag,
)

# Task 2: Second imputation
second_impute_task = PythonOperator(
    task_id='second_impute',
    python_callable=second_impute,
    op_args=[],
    dag=dag,
)

# Task 3: Feature Engineering
feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering,
    op_args=[],
    dag=dag,
)

# Task 4: Train Models
train_models_task = PythonOperator(
    task_id='train_models',
    python_callable=train_models,
    op_args=[],
    dag=dag,
)

################################ Task Dependencies ################################
# Path 1
dummy >> initial_impute_task >> second_impute_task >> feature_engineering_task >> train_models_task

# Path 2

# Model Seelction path