from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# from pyspark.sql import SparkSession
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.functions import col, when, coalesce, lit, mean, udf
# from pyspark.ml import Pipeline
# from pyspark.ml.feature import VectorAssembler, Imputer
# from pyspark.ml.classification import LogisticRegression, LinearSVC
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml.evaluation import BinaryClassificationEvaluator
# from pyspark.sql.types import IntegerType, ArrayType, BooleanType

import os
import boto3
import pandas as pd
import tomli
import time
from typing import List
from io import StringIO
import pathlib

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
aws_access_key_id="ASIAYAAO5HRMGZC4OZO7"
aws_secret_access_key="JRTi+SHnSbWWYQ7oAgzWTbPlJaiZ9X7sPDxVpjVT"
aws_session_token="IQoJb3JpZ2luX2VjEGkaCXVzLWVhc3QtMiJHMEUCIQCU/UkGXx5wUm9w1WdkK+HVEeiEkAzra1OY+PLeqij5ygIgOqE2vWQP1U6osQ7TJkVMBduw9o3hYJV+hM7Tw+aCHKAq9AII8v//////////ARAAGgw1NDk3ODcwOTAwMDgiDDiZVLEeE+AMOqGmLirIArjtIPvvwpBzzUYgN34oB9GSALP/9KzOvfZYhrDz7Hgb1zEbD2FiHnTEZ50qW3nQt7Eelt2xmpMpiI4FPIsrqSg/F4o4tkHYfUJexnvpsYA6u/0hYTuPqbmIkm5R3LA5IoxMbtD+/5d8Xc7YKFQnQ9FH1FFO4U4qMTtikV0hnR6TfoopQ8HGznZacn46avkqGrbrVW5Nmt8m+bt53uFEqd9XNxTSV7wjkBbcNHnx69oVillmWHtIUYBMKSkl/TmB6Tpm6tHY8ksitM7L2a8m2gBahbJ51/ipmh3gVLlNi9Ooe5wrecybaM+f0wt0s9CCm0l9yisYvJdNlDpYaRueHo27Uz+e51llK/Qoeo/iuFKEeErdot5xmCZHHAkX+X7d8TGPGu0WJFE4Bb+7l6UTF+aLsqBSQEL+a86E954LJyU/nNJ1U4sp7hAwqviMswY6pwFVNLdfGic2vjUSPKJPVC2+hJA1q07fmDf1PuTMo1hpepgEOpl6gJYx9fzlsgCpm1DbuWUp+m5wYKYjv5+761AyX2IFCLJc3TxImf/Z3Nej/1FI3MmqmZAv5SXOfOnwFZdlJVU/nZCuxzhwrBP9b3dArcY2S3cKW+Pi1L4qFXfeZRkuqBmx5OSPMywrft8OoFClG0KYnWwLJmjBwgKyOXfW93cNfpC5Iw=="

S3C = boto3.client('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token)
# # On AWS
# S3C = boto3.client('s3')

# CONFIG
CONFIG = read_config_from_s3(S3C)
P1_PATH = "steps_data/p1"
P1_FN1 = "p1_impute_1.csv"
P1_FN2 = "p1_impute_2.csv"
P1_FN3 = "p1_fe.csv"
P1_FN4 = "p1_models.csv"

# Columns, group together numerical, categorical, and binary feature column names
numerical_features = ['age', 'trestbps', 'chol', 'thalach', 'oldpeak', 'tpeakbps', 'trestbpd', 'tpeakbpd', 'thaldur', 'thalrest']
categorical_features = ['restecg']
binary_features = ['sex', 'htn', 'dig', 'prop', 'nitr', 'pro', 'exang', 'xhypo', 'dummy']

mode_features = numerical_features + categorical_features + binary_features

DATA_COLS = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 
           'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']

# Dag Configuration
DAG_ARGS = {
    'owner': 'billyin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

S3_BUCKET = CONFIG['aws']['bucket']
DATA_ROOT_PATH = CONFIG['aws']['bucket']
DATA_PATH = CONFIG['aws']['data_key']

# Path lib join paths
WRITE_PATH_GENERAL = pathlib.Path(S3_BUCKET, CONFIG['aws']['p2_data_folder'])
WRITE_PART_PATH = pathlib.Path(WRITE_PATH_GENERAL, "partitions")
WRITE_DF_CLEANED_PATH = pathlib.Path(WRITE_PATH_GENERAL, "hd_cleaned.csv")

MAX_AGE = 150
TRAIN_TEST_SPLIT = 0.9
NUMBER_OF_FOLDS = 5
DATA_COLS = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 
           'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']


########################################################################################
# HELPER FUNCTIONS FOR P1
########################################################################################
def cprint(msg: str) -> None:
    """
    Function to print messages with a prefix
    """
    print("[CINFO]", msg)

def p1_s3_pk(file_key: str) -> pd.DataFrame:
    """
    Function to construct the S3 path and key for the given file key
    """
    return f"{P1_PATH}/{file_key}"

def s3_pp(filename):
    return f's3://de300spring2024-billyin/emr_related/{filename}'
    
def fetch_and_write_data_db(file_key):
    """
    Decorator function to fetch the data from S3
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Utilize the S3 client to read from S3, the file will be csv
            data = S3C.get_object(Bucket=CONFIG['aws']['bucket'], Key=file_key)
            data = pd.read_csv(data['Body'])

            # Call the function
            new_data = func(data)
            target_key = new_data['target']

            # Number of NA rows
            na_row_count = new_data['data'].isna().any(axis=1).sum()
            cprint(f"Number of rows containing at least one NA value: {na_row_count}")

            # Save data back in S3 by creating an object, key path = P1_PATH + target_key
            csv_buffer = StringIO()
            new_data['data'].to_csv(csv_buffer, index=False)
            S3C.put_object(Bucket=CONFIG['aws']['bucket'], Key=target_key, Body=csv_buffer.getvalue())

            cprint("Data written to S3 successfully")
            cprint(new_data['data'].head())

            # Return the data
            return new_data
        return wrapper
    return decorator

@fetch_and_write_data_db(CONFIG['aws']['data_key'])
def initial_impute_p1(data):
    """
    Function to impute the initial values for the missing data, similar to HW1
    """

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

    cprint("All columns in the 'heart_disease.csv' file:")
    cprint(f"Number of columns: {len(data.columns)}")

    # Convert all object type columns to float64
    for col in data.columns:
        if data[col].dtype == 'object':
            data[col] = data[col].astype('float64')

    cprint("Initial imputation complete")

    # Return the data
    return {
        "data": data,
        "target": p1_s3_pk(P1_FN1)
    }

@fetch_and_write_data_db(p1_s3_pk(P1_FN1))
def second_impute_p1(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to impute the initial values for the missing data, similar to HW1
    """
    cprint("Second imputation started")
    # Remove temporal features
    data.drop(columns=['ekgmo', 'ekgday(day', 'ekgyr', 'cmo', 'cday', 'cyr'], inplace=True)

    # Impute missing values using mode for remaining columns
    for column in mode_features:
        mode_value = data[column].mode().iloc[0]  # Get the mode and handle potential multiple modes
        cprint(f"Imputing mode value for column '{column}': {mode_value}")
        data[column].fillna(mode_value, inplace=True)
        
    # Remove any remaining rows with NA values
    data.dropna(inplace=True)
        
    # Re-evaluate number of rows and columns with at least one NA
    # Identifying columns with any NA values
    columns_with_na = data.columns[data.isna().any()].tolist()
    cprint(f"Columns containing NA values: {columns_with_na}")

    # Counting rows with at least one NA
    na_row_count = data.isna().any(axis=1).sum()
    cprint(f"Number of rows containing at least one NA value: {na_row_count}")

    # Return the cleaned data
    cprint("Second imputation complete")
    return {
        "data": data,
        "target": p1_s3_pk(P1_FN2)
    }

@fetch_and_write_data_db(p1_s3_pk(P1_FN2))
def feature_engineering_p1(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to perform feature engineering on the dataset
    """
    cprint("Feature engineering started")
    # Transform the categorical features into one-hot encoded columns
    data = pd.get_dummies(data, columns=categorical_features)

    print("Data after one-hot encoding of categorical features:")
    print(data.head())

    # Return the transformed data
    cprint("Feature engineering complete")
    return {
        "data": data,
        "target": p1_s3_pk(P1_FN3)
    }

@fetch_and_write_data_db(p1_s3_pk(P1_FN3))
def train_models_p1(data: pd.DataFrame) -> None:
    """
    Function to train the model on the dataset, specifically SVM and Logistic Regression
    """
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
    from sklearn.linear_model import LogisticRegression
    from sklearn.svm import SVC

    cprint("Training models started")
    # Split the data into training and testing sets
    X = data.drop(columns=['target'])
    y = data['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=CONFIG['ml']['test_ratio'], random_state=42)

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

    # Create a dataframe to store the model accuracies
    model_accuracies = pd.DataFrame({
        "Model": ["Logistic Regression P1", "SVM P1"],
        "Accuracy": [lr_score, svm_score]
    })

    # Return the model accuracies
    return {
        "data": model_accuracies,
        "target": p1_s3_pk(P1_FN4)
    }


########################################################################################
# HELPER FUNCTIONS FOR P2
########################################################################################



def final_model_evaluation():
    # Fetch all the model accuracies from S3
    p1_model_accuracies = S3C.get_object(Bucket=CONFIG['aws']['bucket'], Key=p1_s3_pk(P1_FN4))
    p1_model_accuracies = pd.read_csv(p1_model_accuracies['Body'])
    p2_model_accuracies = S3C.get_object(Bucket=CONFIG['aws']['bucket'], Key=p2_s3_pk(P2_FN4))



########################################################################################
# DAG CONFIGURATION
########################################################################################
# Create the DAG
dag = DAG(
    'HW4_DAG',
    default_args=DAG_ARGS,
    description='DAG for Homework 4',
    schedule_interval=CONFIG['workflow']['workflow_schedule_interval'],
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
    python_callable=initial_impute_p1,
    op_args=[],
    dag=dag,
)

# Task 2: Second imputation
second_impute_task = PythonOperator(
    task_id='second_impute',
    python_callable=second_impute_p1,
    op_args=[],
    dag=dag,
)

# Task 3: Feature Engineering
feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering_p1,
    op_args=[],
    dag=dag,
)

# Task 4: Train Models
train_models_task = PythonOperator(
    task_id='train_models',
    python_callable=train_models_p1,
    op_args=[],
    dag=dag,
)

########################################################################################
# DAG TASKS
########################################################################################
# Path 1
initial_impute_task >> second_impute_task >> feature_engineering_task >> train_models_task


# Model Seelction path