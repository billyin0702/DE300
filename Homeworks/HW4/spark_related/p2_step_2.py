# File for performing HW2 processes but on Spark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, coalesce, lit, mean
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.classification import LogisticRegression, LinearSVC
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import os
import time
import tomli
import pathlib
import pandas as pd
import boto3

from typing import List

######################################
# Overview
######################################

# 1. Read the cleaned data from Step 1
# 2. Clean the data further using techniques like imputation and one-hot encoding 
# 3. Evaluate the models using the cleaned data

######################################
# Configs
######################################

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

######################################
# Constants
######################################
S3C = boto3.client('s3')
CONFIG = read_config_from_s3(S3C)

S3_BUCKET = CONFIG['aws']['bucket']
DATA_ROOT_PATH = CONFIG['aws']['bucket']
DATA_PATH = CONFIG['aws']['data_key']

# Path lib join paths
WRITE_PATH_GENERAL = pathlib.Path(S3_BUCKET, CONFIG['aws']['merge_data_folder'])
WRITE_DF_CLEANED_PATH = pathlib.Path(WRITE_PATH_GENERAL, "hd_cleaned.csv")
WRITE_MODEL_DF = pathlib.Path(S3_BUCKET, "steps_data", "merge")

MAX_AGE = 150
TRAIN_TEST_SPLIT = 1 - CONFIG['ml']['test_ratio']
NUMBER_OF_FOLDS = 5
DATA_COLS = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 
           'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']


######################################
# MISC HELPER FUNCTIONS
######################################

def cprint(text: str):
    """
    Custom print function
    """
    print(f"[CINFO] {text}")

######################################
# SPARK HELPER FUNCTIONS
######################################
def read_step1_data(spark: SparkSession):
    """
    Read the data
    """
    data = S3C.get_object(Bucket=S3_BUCKET, Key=WRITE_DF_CLEANED_PATH)
    data = pd.read_csv(data['Body'])

    # Transform the data to a Spark DataFrame
    data = spark.createDataFrame(data)
    
    return data

def write_data_to_s3(data):
    """
    Write the data to a directory
    """
    # Write the data to a CSV file onto S3
    data.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(WRITE_MODEL_DF)

def fill_with_mode(data:DataFrame, column):
    """
    Fill the missing values in a column with the mode
    """
    # Find the mode (most frequent value)
    mode = data.groupBy(column).count().orderBy("count", ascending=False).first()[0]

    # Fill missing values with the mode
    data = data.na.fill({column: mode})
    return data

def fill_with_mean(data: DataFrame, column):
    """
    Fill the missing values in a column with the mean
    """
    # Find the mean
    # Calculate the mean of the specified column
    mean_value = data.select(mean(col(column)).alias("mean")).collect()[0]["mean"]
    # Fill missing values with the calculated mean
    data = data.na.fill({column: mean_value})
    return data

######################################
# SPARK MAIN FUNCTIONS
######################################
def clean_data_advanced(spark: SparkSession, data: DataFrame):
    imputers = []
    imputed_columns = []
    cols = DATA_COLS

    # Clean the data further using techniques like imputation and one-hot encoding
    # 1. Replace painloc and painexer NAs with 0 (default value)
    data = data.withColumn("painloc", coalesce(col("painloc"), lit(0)))
    data = data.withColumn("painexer", coalesce(col("painexer"), lit(0)))

    # 2. CP is a categorical variable, remove all NA rows
    data = data.na.drop(subset=["cp"])

    # 3. Trestbps is a continuous variable, replace NAs and <= 100 with's the mode
    data = fill_with_mode(data, "trestbps")
    data = data.withColumn("trestbps", when(col("trestbps") <= 100, 130).otherwise(col("trestbps")))

    # 4. Clean the smoke column by filling with the mode
    data = fill_with_mode(data, "smoke")

    # 5. Replace fbs, prop, nitr, pro, diuretic NAs and values greater than one with 0
    data = data.withColumn("fbs", coalesce(col("fbs"), lit(0)))
    data = data.withColumn("prop", coalesce(col("prop"), lit(0)))
    data = data.withColumn("nitr", coalesce(col("nitr"), lit(0)))
    data = data.withColumn("pro", coalesce(col("pro"), lit(0)))
    data = data.withColumn("diuretic", coalesce(col("diuretic"), lit(0)))

    # 6. Use imputer to fill missing values
    imputer_1 = Imputer(inputCols=["thaldur", "thalach"], outputCols=["thaldur_imputed", "thalach_imputed"], strategy = "mean")
    imputers.append(imputer_1)
    imputed_columns.extend(["thaldur_imputed", "thalach_imputed"])
    cols.remove("thaldur")
    cols.remove("thalach")

    # 7. Exang is a binary variable, replace NAs with 0
    data = data.withColumn("exang", coalesce(col("exang"), lit(0)))

    # 8. Oldpeak is a continuous variable, replace NAs, larger or equal to 4, less or equal to 0 with the mean
    data = data.withColumn("oldpeak", when(col("oldpeak") >= 4, 1.5).otherwise(col("oldpeak")))
    data = data.withColumn("oldpeak", when(col("oldpeak") <= 0, 1.5).otherwise(col("oldpeak")))
    data = fill_with_mean(data, "oldpeak")

    # 9. Use imputer to fill missing values
    imputer_2 = Imputer(inputCols=["slope"], outputCols=["slope_imputed"], strategy="mode")
    imputers.append(imputer_2)
    imputed_columns.append("slope_imputed")
    cols.remove("slope")

    # 10. Review all columns to ensure no NAs
    cprint(f"Number of Rows: {data.count()}")
    cprint(f"NA Review")
    data.select([col(c).alias(c) for c in data.columns if data.where(col(c).isNull()).count() > 0]).show()

    return data, imputers, imputed_columns, cols

def evaluate_models(data: DataFrame, imputers: List[Imputer], imputed_cols: List[str], cols: List[str]):
    cprint("Evaluating models...")
    # Assembler for the features
    input_cols = imputed_cols + cols
    input_cols.remove("target")
    assembler = VectorAssembler(
        inputCols=input_cols, 
        outputCol="features"
        )
    
    # Define a list of classifiers to evaluate
    classifiers = [
        LogisticRegression(labelCol="target", featuresCol="features"),
        LinearSVC(labelCol="target", featuresCol="features"),
    ]

    # Define the parameter grid for each classifier
    defined_params = [
        ["maxIter", "regParam", "elasticNetParam", "tol"],
        ["maxIter", "regParam", "tol"]
    ]

    param_grids = [
        ParamGridBuilder() \
            .addGrid(classifiers[0].maxIter, [20, 40]) \
            .addGrid(classifiers[0].regParam, [0.01, 0.1]) \
            .addGrid(classifiers[0].elasticNetParam, [0.0, 0.5]) \
            .addGrid(classifiers[0].tol, [1e-6, 1e-4]) \
            .build(),
        ParamGridBuilder() \
            .addGrid(classifiers[1].maxIter, [20, 40]) \
            .addGrid(classifiers[1].regParam, [0.01, 0.1]) \
            .addGrid(classifiers[1].tol, [1e-6, 1e-4]) \
            .build()
    ]
    # Split the data into training and testing sets
    train_data, test_data = data.randomSplit([TRAIN_TEST_SPLIT, 1-TRAIN_TEST_SPLIT])

    # Record the model and accuracy
    model_accuracies = []
    aucs = []

    # Evaluate model on each classifier and print out the time taken for each one
    for i, classifier in enumerate(classifiers):
        # Start the timer
        cprint(f"Starting evaluation for {classifier.__class__.__name__}")
        start = time.time()
        # Create the pipeline
        pipeline = Pipeline(stages=[*imputers, assembler, classifier])
        # Call the evaluate_model function
        model_name, auc = evaluate_model(train_data, test_data, pipeline, param_grids[i], classifier.__class__.__name__, defined_params[i])
        # End the timer
        end = time.time()
        cprint(f"Time taken: {end - start:.2f} seconds")
        # Append the model and accuracy to the list
        model_accuracies.append((model_name + " P2", auc))
        aucs.append(auc)

    # Create a DataFrame to store the model accuracies
    model_accuracies_df = pd.DataFrame(model_accuracies, columns=["Model", "Accuracy"])
    
    # Write the model accuracies to a CSV file on S3
    write_data_to_s3(model_accuracies_df)


def evaluate_model(train_data: DataFrame, test_data: DataFrame, pipeline: Pipeline, paramGrid: ParamGridBuilder, model_name: str, parameters: List[str]):
    """
    Evaluate the model
    """
    # Set up the cross-validator
    evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    crossval = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=NUMBER_OF_FOLDS)

    # Fit the pipeline
    cvModel = crossval.fit(train_data)

    # Make predictions
    predictions = cvModel.transform(test_data)

    # Evaluate the model
    auc = evaluator.evaluate(predictions)

    # Get the best mode
    best_model = cvModel.bestModel.stages[-1]

    # Print the best values
    print()
    cprint("##################### Results #####################")
    cprint(f"Model: {model_name}")
    cprint(f"Area Under ROC Curve: {auc:.4f}")
    # Retrive the best parameters
    for param in parameters:
        cprint(f"Best {param}: {best_model.getOrDefault(best_model.getParam(param))}")
    cprint("##################################################")
    print()

######################################
# MAIN FUNCTION
######################################

def main():
    cprint("Starting the process...")
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Further Cleaning and Modeling") \
        .getOrCreate()
    
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    cprint("Reading data...")

    # Read the data
    data = read_step1_data(spark)
    cprint("Read data!")

    # Clean the data further from Step 1
    data, imputers, imputed_cols, cols = clean_data_advanced(spark, data)
    cprint("Advanced cleaning done!")

    # Pipeline to evaluate
    evaluate_models(data, imputers, imputed_cols, cols)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()