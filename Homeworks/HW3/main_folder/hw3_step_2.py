# File for performing HW2 processes but on Spark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, coalesce, lit, mean
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier, DecisionTreeClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import os
import time

from typing import List

# ######################################
# # Constants
# ######################################

ROOT_PATH = "data"
# ROOT_PATH = "s3://de300spring2024/bill_yin/hw3_spark/actual_homework/data/"
STEP1_CLEANED_DATA_PATH = "hd_cleaned.csv"
WRITE_PART_PATH = "hd_final/"
OUTPUT_PATH = "hd_final.csv"

MAX_AGE = 150
TRAIN_TEST_SPLIT = 0.9
NUMBER_OF_FOLDS = 5
DATA_COLS = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 
           'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']

SPLIT_SEED = 7576


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
    # Read the dataset from a CSV file, but remove the c0_ prefix from the column names
    data = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(os.path.join(ROOT_PATH, STEP1_CLEANED_DATA_PATH))
    
    return data

def write_data(data):
    """
    Write the data to a directory
    """
    # Write the data partitinos to a directory
    data.write.csv(ROOT_PATH + WRITE_PART_PATH, mode="overwrite")
    cprint("Wrote all partitions to a directory")
    # Convert the data to a Pandas DataFrame and write to a CSV file
    df = data.toPandas()
    df.to_csv(ROOT_PATH + OUTPUT_PATH, index=False)
    cprint("Wrote file")

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

    # 4. Clean the smoke column
    # NOTE: Smoke will be cleaned separately to avoid dependency issues

    # 5. Replace fbs, prop, nitr, pro, diuretic NAs and values greater than one with 0
    data = data.withColumn("fbs", coalesce(col("fbs"), lit(0)))
    data = data.withColumn("prop", coalesce(col("prop"), lit(0)))
    data = data.withColumn("nitr", coalesce(col("nitr"), lit(0)))
    data = data.withColumn("pro", coalesce(col("pro"), lit(0)))
    data = data.withColumn("diuretic", coalesce(col("diuretic"), lit(0)))

    # # 6. Thaldur and Thalach are continuous variables, replace NAs with the mean
    # data = fill_with_mean(data, "thaldur")
    # data = fill_with_mean(data, "thalach")

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

    # # 9. Slope is a categorical variable, replace NAs with the mode
    # data = fill_with_mode(data, "slope")

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
        RandomForestClassifier(labelCol="target", featuresCol="features"),
        LogisticRegression(labelCol="target", featuresCol="features"),
        GBTClassifier(labelCol="target", featuresCol="features"),
        DecisionTreeClassifier(labelCol="target", featuresCol="features")
    ]

    # Define the parameter grid for each classifier
    defined_params = [
        ["maxDepth", "numTrees", "maxBins", "featureSubsetStrategy"],
        ["maxIter", "regParam", "elasticNetParam", "tol"],
        ["maxDepth", "maxIter", "maxBins", "stepSize"],
        ["maxDepth", "maxBins", "minInfoGain", "minInstancesPerNode"]
    ]

    param_grids = [
        ParamGridBuilder() \
            .addGrid(classifiers[0].maxDepth, [4, 8]) \
            .addGrid(classifiers[0].numTrees, [10, 100, 1000]) \
            .addGrid(classifiers[0].maxBins, [10, 50]) \
            .addGrid(classifiers[0].featureSubsetStrategy, ["sqrt", "log2"]) \
            .build(),
        ParamGridBuilder() \
            .addGrid(classifiers[1].maxIter, [20, 40]) \
            .addGrid(classifiers[1].regParam, [0.01, 0.1]) \
            .addGrid(classifiers[1].elasticNetParam, [0.0, 0.5]) \
            .addGrid(classifiers[1].tol, [1e-6, 1e-4]) \
            .build(),
        ParamGridBuilder() \
            .addGrid(classifiers[2].maxDepth, [4, 8]) \
            .addGrid(classifiers[2].maxIter, [20, 40]) \
            .addGrid(classifiers[2].maxBins, [20, 40]) \
            .addGrid(classifiers[2].stepSize, [0.1, 0.01]) \
            .build(),
        ParamGridBuilder() \
            .addGrid(classifiers[3].maxDepth, [4, 8]) \
            .addGrid(classifiers[3].maxBins, [20, 40]) \
            .addGrid(classifiers[3].minInfoGain, [0.0, 0.1]) \
            .addGrid(classifiers[3].minInstancesPerNode, [1, 10, 20]) \
            .build()
    ]
    # Split the data into training and testing sets
    train_data, test_data = data.randomSplit([TRAIN_TEST_SPLIT, 1-TRAIN_TEST_SPLIT], seed=SPLIT_SEED)

    # Evaluate model on each classifier and print out the time taken for each one
    for i, classifier in enumerate(classifiers):
        # Start the timer
        cprint(f"Starting evaluation for {classifier.__class__.__name__}")
        start = time.time()
        # Create the pipeline
        pipeline = Pipeline(stages=[*imputers, assembler, classifier])
        # Call the evaluate_model function
        evaluate_model(train_data, test_data, pipeline, param_grids[i], classifier.__class__.__name__, defined_params[i])
        # End the timer
        end = time.time()
        cprint(f"Time taken: {end - start:.2f} seconds")

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
        numFolds=NUMBER_OF_FOLDS,
        seed=SPLIT_SEED)

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