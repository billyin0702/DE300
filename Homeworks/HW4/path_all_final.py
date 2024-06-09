from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import boto3
import pandas as pd
from io import StringIO
import tomli
import numpy as np

########################################################################################
# CONFIG
########################################################################################
CONFIG_FILE_BUCKET = "de300spring2024-billyin"
CONFIG_FILE_KEY = "config.toml"
TRAIN_TEST_SPLIT = 0.9
NUMBER_OF_FOLDS = 5
MAX_AGE = 150

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
    
# CONFIG
S3C = boto3.client('s3')
CONFIG = read_config_from_s3(S3C)

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

########################################################################################
# HELPER FUNCTIONS MISC
########################################################################################
def cprint(msg: str) -> None:
    """
    Function to print messages with a prefix
    """
    print("[CINFO]", msg)

def fill_with_mode(data, column):
    """
    Fill the missing values in a column with the mode
    """
    # Find the mode (most frequent value)
    mode = data.groupBy(column).count().orderBy("count", ascending=False).first()[0]

    # Fill missing values with the mode
    data = data.na.fill({column: mode})
    return data


########################################################################################
# PATH 1
########################################################################################
def read_data_start(**kwargs):
    """
    Read the data
    """
    data = S3C.get_object(Bucket=S3_BUCKET, Key=DATA_PATH)
    data = pd.read_csv(data['Body'])
    
    return data

def initial_impute_p1(**kwargs):
    """
    Function to impute the initial values for the missing data, similar to HW1
    """
    data = kwargs['ti'].xcom_pull(task_ids='read_data')
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
    cprint("More imputation complete")

    # Return the data
    return data

def feature_engineering_p1(**kwargs) -> pd.DataFrame:
    """
    Function to perform feature engineering on the dataset
    """
    cprint("Feature engineering started")
    data = kwargs['ti'].xcom_pull(task_ids='initial_impute_p1')
    # Transform the categorical features into one-hot encoded columns
    data = pd.get_dummies(data, columns=categorical_features)

    # Return the transformed data
    cprint("Feature engineering complete")
    return data

###############################################################################
# Path 2
###############################################################################

def initial_impute_p2(**kwargs):
    """
    Clean the data
    """
    # Add the necesasry imports
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, udf, when
    from pyspark.sql.types import BooleanType, StructType, StructField, StringType

    # Create a Spark session
    spark = SparkSession.builder.appName("Initial_impute_2").getOrCreate()
    data = kwargs['ti'].xcom_pull(task_ids='read_data')
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Select the columns to use
    data = data.select(DATA_COLS)

    # Perform validity checks
    # Function to check if any value in a row contains spaces or is non-numeric
    # Specifically if they are of type int or float
    def check_validity(item):
        overall_valid = False

        # Check if the item is an int
        try:
            int(item)
            overall_valid = True
        except ValueError:
            pass
        except TypeError:
            pass
        
        # Check if the item is a float
        try:
            float(item)
            overall_valid = True
        except ValueError:
            pass
        except TypeError:
            pass
        
        # Check if the item is a None
        if item is None:
            overall_valid = True

        return overall_valid
    
    # Convert to user-defined function
    check_validity_udf = udf(check_validity, BooleanType())

    # Apply the function to each row
    for column in DATA_COLS:
        data = data.withColumn(f"{column}_is_valid", check_validity_udf(col(column)))
        data = data.filter(col(f"{column}_is_valid"))

    # Remove auxiliary columns
    for column in DATA_COLS:
        data = data.drop(f"{column}_is_valid")
    
    # Remove rows with more than 20% of missing values
    threshold = int(0.7 * len(DATA_COLS))
    data = data.dropna(thresh=threshold)

    # Convert age to integer, handling non-null values safely
    data = data.withColumn("age", when(col("age").isNotNull(), col("age").cast("int")))

    # Fill with mode
    data = fill_with_mode(data, "age")

    # Turn data back to pandas
    data = data.toPandas()
    return data

######################################
# SPARK HELPER FUNCTIONS
######################################
def fill_with_mode(data, column):
    """
    Fill the missing values in a column with the mode
    """
    # Find the mode (most frequent value)
    mode = data.groupBy(column).count().orderBy("count", ascending=False).first()[0]

    # Fill missing values with the mode
    data = data.na.fill({column: mode})
    return data

def fill_with_mean(data, column):
    """
    Fill the missing values in a column with the mean
    """
    from pyspark.sql.functions import mean, col

    # Find the mean
    # Calculate the mean of the specified column
    mean_value = data.select(mean(col(column)).alias("mean")).collect()[0]["mean"]
    # Fill missing values with the calculated mean
    data = data.na.fill({column: mean_value})
    return data

def feature_engineering_p2(**kwargs):
    # Necessary Imports
    from pyspark.sql.functions import coalesce, lit
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when
    from pyspark.sql.types import StringType, StructType, StructField, DoubleType

    data = kwargs['ti'].xcom_pull(task_ids='initial_impute_p2')

    # Turn data into a Spark DataFrame
    spark = SparkSession.builder.appName("Feature_Engineering_2").getOrCreate()
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Cast the columns to doubles
    for col_name in data.columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # imputers = []
    # imputed_columns = []
    # cols = DATA_COLS

    # Clean the data further using techniques like imputation and one-hot encoding
    # 1. Replace painloc and painexer NaNs with 0 (default value)
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

    # 6. Thaldur and Thalach are continuous variables, replace NAs with the mean
    data = fill_with_mean(data, "thaldur")
    data = fill_with_mean(data, "thalach")
    # # 6. Use imputer to fill missing values
    # # imputer_1 = Imputer(inputCols=["thaldur", "thalach"], outputCols=["thaldur_imputed", "thalach_imputed"], strategy = "mean")
    # # imputers.append(imputer_1)
    # imputed_columns.extend(["thaldur_imputed", "thalach_imputed"])
    # cols.remove("thaldur")
    # cols.remove("thalach")

    # 7. Exang is a binary variable, replace NAs with 0
    data = data.withColumn("exang", coalesce(col("exang"), lit(0)))

    # 8. Oldpeak is a continuous variable, replace NAs, larger or equal to 4, less or equal to 0 with the mean
    data = data.withColumn("oldpeak", when(col("oldpeak") >= 4, 1.5).otherwise(col("oldpeak")))
    data = data.withColumn("oldpeak", when(col("oldpeak") <= 0, 1.5).otherwise(col("oldpeak")))
    data = fill_with_mean(data, "oldpeak")

    # 9. Slope is a categorical variable, replace NAs with the mode
    data = fill_with_mode(data, "slope")

    # # 9. Use imputer to fill missing values
    # # imputer_2 = Imputer(inputCols=["slope"], outputCols=["slope_imputed"], strategy="mode")
    # # imputers.append(imputer_2)
    # imputed_columns.append("slope_imputed")
    # cols.remove("slope")

    # 10. Review all columns to ensure no NAs
    cprint(f"Number of Rows: {data.count()}")
    cprint(f"NA Review")
    data.select([col(c).alias(c) for c in data.columns if data.where(col(c).isNull()).count() > 0]).show()

    # Cprint number of na rows
    cprint(f"Number of NA rows: {data.na.drop().count()}")

    # Turn data back to pandas
    data = data.toPandas()
    return data

########################################################################################
# MERGE
########################################################################################
# Merge the results from feature engineering 2 by applying smoke replacements
def scrape_smoke(**kwargs):
    # Necessary Imports
    from scrapy import Selector
    import requests
    import re
    import numpy as np
    import pandas as pd
    from typing import List

    def load_web_selector(url: str) -> Selector:
        """
        Load a webpage and return a Selector object for parsing
        """
        # Load the webpage
        r = requests.get(url)
        r.raise_for_status()

        # Create the Selector object
        return Selector(text=r.content)

    def parse_row(row:Selector) -> List[str]:
        """
        Parses a html row into a list of individual elements
        """

        cells = row.xpath('.//th | .//td')
        row_data = []
        
        for cell in cells:
            cell_text = cell.xpath('normalize-space(.)').get()
            cell_text = re.sub(r'<.*?>', ' ', cell_text)  # Remove remaining HTML tags
            # if there are br tags, there will be some binary characters
            cell_text = cell_text.replace('\xa0', '')  # Remove \xa0 characters
            row_data.append(cell_text)

        if len(row_data) == 1:
            row_data.append(MAX_AGE)
        
        return row_data

    def extract_percentage(text):
        """
        Extract a percentage from a string and return it as a float
        """
        # Define the regex pattern for finding a number followed by a percent sign
        pattern = r"\d+(\.\d+)?%"
        
        # Search the text for the pattern
        match = re.search(pattern, text)
        
        # Check if a match was found
        if match:
            # Extract the matched text and remove the percent sign
            percentage_str = match.group(0)[:-1]
            # Convert the extracted string to a float
            percentage_float = float(percentage_str)

            return percentage_float
        else:
            # Return None or raise an error if no percentage was found
            return None
        
    def extract_specific_ages(text):
        """
        Extract the specific ages from a string and return them as a list of integers
        """
        # Use regex to find the part of the text with the age range
        age_pattern = r"aged (\d+)–(\d+)"
        match = re.search(age_pattern, text)
        if match:
            # Extract only the age range part, which are the first two groups in the match
            return tuple(map(int, match.groups()))
        return None
    
    def load_web_selector(url: str) -> Selector:
        """
        Load a webpage and return a Selector object for parsing
        """
        # Load the webpage
        r = requests.get(url)
        r.raise_for_status()

        # Create the Selector object
        return Selector(text=r.content)

    # Scrape smoking data
    abs_gov_selector = load_web_selector("https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/latest-release")
    cdc_gov_selector = load_web_selector("https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm")

    # Select all the tables from the abs page
    abs_tables = abs_gov_selector.xpath("//table")

    # Select the div with the smoking data
    cdc_divs = cdc_gov_selector.xpath("//div[@class='card border-0 rounded-0 mb-3']")

    # Select the table and divs
    abs_table = abs_tables[1]
    cdc_sex = cdc_divs[0]
    cdc_age = cdc_divs[1]

    # Parse the rows from the abs table
    # Table = table, header = thead, rows = tbody
    abs_header = abs_table.xpath(".//thead//tr")
    abs_body = abs_table.xpath(".//tbody//tr")

    abs_header = parse_row(abs_header[0])
    abs_header[0] = "Age Group"
    abs_rows = [parse_row(row) for row in abs_body[1:]]

    # Parse the cdc data
    cdc_sex = cdc_sex.xpath(".//li/text()").getall()
    cdc_age = cdc_age.xpath(".//li/text()").getall()

    # Extract the sex and age percentage inside the paranthese inside the string
    cdc_data = {"sex":{}, "ages":{}}
    cdc_data["sex"]['male'] = extract_percentage(cdc_sex[0])
    cdc_data["sex"]['female'] = extract_percentage(cdc_sex[1])
    cdc_data["ages"][extract_specific_ages(cdc_age[0])] = extract_percentage(cdc_age[0])
    cdc_data["ages"][extract_specific_ages(cdc_age[1])] = extract_percentage(cdc_age[1])
    cdc_data["ages"][extract_specific_ages(cdc_age[2])] = extract_percentage(cdc_age[2])
    cdc_data["ages"][tuple([65])] = extract_percentage(cdc_age[3])

    # Alter data to be the following:
    # Female (0) - Corresponding age group percentage
    # Male   (1) - Corresponding age group percentage * smoking rate among men / smoking rate among women
    cdc_alt_data = {}
    cdc_alt_data[0] = cdc_data['ages']
    cdc_alt_data[1] = {}
    for age_range in cdc_data['ages']:
        cdc_alt_data[1][age_range] = cdc_data['ages'][age_range] * cdc_data['sex']['male'] / cdc_data['sex']['female']

    # Send the data to XCom
    return abs_rows, abs_header, cdc_alt_data


def merge_smoke(**kwargs):
    # Necessary Imports
    import re
    import numpy as np
    import pandas as pd
    from pyspark.sql.functions import udf, col, when
    from pyspark.sql.types import IntegerType, ArrayType, BooleanType, StringType, StructType, StructField, DoubleType
    from pyspark.sql import SparkSession

    def extract_numbers(text):
        """
        Extract all the numbers from a string and return them as a list of integers
        """
        # Regex pattern to match digits possibly separated by an en dash, em dash, or hyphen
        pattern = r"\d+"
        
        # Find all matches of the pattern
        matches = re.findall(pattern, text)
        
        # Convert the matched strings to integers
        numbers = list(map(int, matches))
        return numbers

    def smoke_assign(prob: float) -> int:
        return int(np.random.rand() < prob)

    # Define a UDF to check if age falls within any given range
    def age_in_range(age, rg):
        age = float(age)
        try: 
            rg = list(map(float, rg))
            if len(rg) == 1:
                return age >= rg[0]
            return rg[0] <= age <= rg[1]
        except:
            return True

    def same_gender(g1: int, g2: int):
        return float(g1) == float(g2)

    """
    Clean the smoke column by removing the 'smoke' prefix
    """

    # New Spark Session
    spark = SparkSession.builder.appName("Smoke_Scrape").getOrCreate()

    # Get Data from the previous task
    data = kwargs['ti'].xcom_pull(task_ids='feature_engineering_p2')
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    cprint(f"All columns: {data.columns}")
    data = spark.createDataFrame(data, schema=schema)
    abs_rows, abs_headers, cdc_alt_data = kwargs['ti'].xcom_pull(task_ids='scrape_smoke')

    # Create a Spark DataFrame from the abs_rows and abs_headers
    abs_data = spark.createDataFrame(abs_rows, abs_headers)

    # Assign UDFs
    extract_numbers_udf = udf(extract_numbers, ArrayType(IntegerType()))
    smoke_assign_udf = udf(smoke_assign, IntegerType())
    age_in_range_udf = udf(age_in_range, BooleanType())
    same_gender_udf = udf(same_gender, BooleanType())

    # Assuming abs_data is loaded and has the 'Age Group' and '2022 (%)' columns
    abs_data = abs_data.withColumn("ages", extract_numbers_udf("Age Group"))
    abs_data = abs_data.withColumn("prob", col("2022 (%)") / 100)
    abs_data = abs_data.select("ages", "prob")

    # Join data with age_ranges_df using the UDF to check range inclusion
    data = data.crossJoin(abs_data).filter(age_in_range_udf(col("age"), col("ages")))

    # Apply the smoke_assign UDF based on the joined prob values
    data = data.withColumn("smoke_abs", smoke_assign_udf(col("prob")))

    # Optionally, fill remaining nulls in 'smoke_abs'
    data = data.fillna({"smoke_abs": 0})

    # Assuming cdc_alt_data is structured similarly or loaded appropriately
    # Make a df from the cdc_alt_data dictionary with columns: gender, age_range, prob
    cdc_alt_data_list = []
    for gender in cdc_alt_data:
        for age_range in cdc_alt_data[gender]:
            cdc_alt_data_list.append([int(gender), list(age_range), cdc_alt_data[gender][age_range]])

    cdc_alt_data_df = spark.createDataFrame(cdc_alt_data_list, ['gender_cdc', 'ages_cdc', 'prob_cdc_100'])
    cdc_alt_data_df = cdc_alt_data_df.withColumn("prob_cdc", col("prob_cdc_100") / 100)

    # Now cross-join, and filter like above
    data = data.crossJoin(cdc_alt_data_df)\
        .filter(same_gender_udf(col('sex'), col('gender_cdc'))) \
        .filter(age_in_range_udf(col("age"), col("ages_cdc")))
    
    # Apply the smoke_assign UDF based on the joined prob values
    data = data.withColumn("smoke_cdc", smoke_assign_udf(col("prob_cdc")))

    # Optionally, fill remaining nulls in 'smoke_cdc'
    data = data.fillna({"smoke_cdc": 0})


    # Aggergate both columns, if any of them are 1, then the person smokes
    # Do so only for rows that have NA values in the smoke column
    data = data.withColumn("smoke", \
            when(col("smoke").isNull() & ((col("smoke_abs") == 1) | (col("smoke_cdc") == 1)), 1)\
            .otherwise(when(col("smoke").isNull(), 0).otherwise(col("smoke"))))

    # Drop auxiliary columns
    data = data.drop("smoke_abs", "smoke_cdc", "ages", "prob", "ages_cdc", "prob_cdc", "prob_cdc_100", "gender_cdc")

    # Turn data back to pandas
    data = data.toPandas()
    return data


########################################################################################
# EVALUATE MODELS
########################################################################################
def evaluate_models_p1_helper(data, model_name: str) -> pd.DataFrame:
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
    from sklearn.linear_model import LogisticRegression
    from sklearn.svm import SVC

    # Split the data into training and testing sets
    X = data.drop(columns=['target'])
    y = data['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=CONFIG['ml']['test_ratio'], random_state=42)

    # Train the Logistic Regression model
    lr_model = LogisticRegression(max_iter=1000)
    lr_model.fit(X_train, y_train)
    lr_score = accuracy_score(y_test, lr_model.predict(X_test))
    print(f"Logistic Regression model accuracy: {lr_score}")

    # Train the model
    if model_name == "SVM":
        model = SVC()
    else:
        model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    score = accuracy_score(y_test, model.predict(X_test))

    return score

def evaluate_models_p1_lr(**kwargs) -> None:
    """
    Wrapper function to evaluate the Logistic Regression model
    """

    data = kwargs['ti'].xcom_pull(task_ids='feature_engineering_p1')
    model_accuracies = evaluate_models_p1_helper(data, "Logistic Regression")

    return {
        "model": "p1_lr",
        "accuracy": model_accuracies
    }

def evaluate_models_p1_svm(**kwargs) -> None:
    """
    Wrapper function to evaluate the SVM model
    """

    data = kwargs['ti'].xcom_pull(task_ids='feature_engineering_p1')
    model_accuracies = evaluate_models_p1_helper(data, "SVM")

    return {
        "model": "p1_svm",
        "accuracy": model_accuracies
    }


def evaluate_lr_model_spark(data):
    # Necessary Imports
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.tuning import ParamGridBuilder
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import Imputer
    from pyspark.sql.functions import col

    cprint("Evaluating models...")
    # Assembler for the features
    input_cols = DATA_COLS
    input_cols.remove("target")
    assembler = VectorAssembler(
        inputCols=input_cols, 
        outputCol="features"
        )
    
    # Remove any remaining rows with NA values
    cprint(f"Rows before dropping: {data.count()}")
    data = data.dropna()
    cprint(f"Rows after dropping: {data.count()}")
    
    # Print number of NA rows
    cprint(f"Number of NA rows: {data.na.drop().count()}")

    lr = LogisticRegression(featuresCol='features', 
                            labelCol='target', 
                            maxIter=20, 
                            regParam=0, 
                            elasticNetParam=0.8,
                            tol=1e-6)

    # Split the data into training and testing sets
    train_data, test_data = data.randomSplit([TRAIN_TEST_SPLIT, 1-TRAIN_TEST_SPLIT])

    # Create the pipeline
    cprint("Creating pipeline...")
    pipeline = Pipeline(stages=[assembler, lr])

    # Fit the model
    cprint("Fitting model...")
    model = pipeline.fit(train_data)

    # Make predictions
    cprint("Making predictions...")
    predictions = model.transform(test_data)

    # Evaluate the model
    evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

    # Get the AUC
    auc = evaluator.evaluate(predictions)

    return auc

def evaluate_svc_model_spark(data):
    # Necessary Imports
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LinearSVC
    from pyspark.ml.tuning import ParamGridBuilder
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import Imputer
    from pyspark.sql.functions import col

    cprint("Evaluating models...")
    # Assembler for the features
    input_cols = DATA_COLS
    input_cols.remove("target")
    assembler = VectorAssembler(
        inputCols=input_cols, 
        outputCol="features"
        )
    
    # Remove any remaining rows with NA values
    cprint(f"Rows before dropping: {data.count()}")
    data = data.dropna()
    cprint(f"Rows after dropping: {data.count()}")
    
    # Print number of NA rows
    cprint(f"Number of NA rows: {data.na.drop().count()}")

    svc = LinearSVC(featuresCol='features', 
                            labelCol='target')

    # Split the data into training and testing sets
    train_data, test_data = data.randomSplit([TRAIN_TEST_SPLIT, 1-TRAIN_TEST_SPLIT])

    # Create the pipeline
    pipeline = Pipeline(stages=[assembler, svc])

    # Fit the model
    model = pipeline.fit(train_data)

    # Make predictions
    predictions = model.transform(test_data)

    # Evaluate the model
    evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

    # Get the AUC
    auc = evaluator.evaluate(predictions)

    # Return the name of the model and the AUC
    return auc


def evaluate_models_p2_lr(**kwargs):
    """
    Wrapper function to evaluate the Logistic Regression model
    """
    # Imports
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql.functions import col

    # Get Data
    data = kwargs['ti'].xcom_pull(task_ids='feature_engineering_p2')

    # Turn data into a Spark DataFrame
    spark = SparkSession.builder.appName("Evaluate_Models_P2_LR").getOrCreate()
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Cast the columns to doubles
    for col_name in data.columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # Print numbers of rows remaining after dropping NAs
    # cprint(f"Rows before dropping: {data.count()}")
    # data = data.dropna()
    # cprint(f"Rows after dropping: {data.count()}")

    # Evaluate the model
    model_accuracy = evaluate_lr_model_spark(data)

    return {
        "model": "p2_lr",
        "accuracy": model_accuracy
    }

def evaluate_models_p2_svm(**kwargs):
    """
    Wrapper function to evaluate the SVM model
    """
    # Imports
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql.functions import col

    # Get Data
    data = kwargs['ti'].xcom_pull(task_ids='feature_engineering_p2')

    # Turn data into a Spark DataFrame
    spark = SparkSession.builder.appName("Evaluate_Models_P2_SVM").getOrCreate()
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Cast the columns to doubles
    for col_name in data.columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # Remove any remaining rows with NA values
    # cprint(f"Rows before dropping: {data.count()}")
    # data = data.dropna()
    # cprint(f"Rows after dropping: {data.count()}")

    # Evaluate the model
    model_accuracy = evaluate_svc_model_spark(data)

    return {
        "model": "p2_svm",
        "accuracy": model_accuracy
    }

def evaluate_models_merge_lr(**kwargs):
    """
    Wrapper function to evaluate the Logistic Regression model
    """
    # Imports
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql.functions import col

    # Get Data
    # Get Data
    data = kwargs['ti'].xcom_pull(task_ids='merge_smoke')

    # Turn data into a Spark DataFrame
    spark = SparkSession.builder.appName("Evaluate_Models_Merge_LR").getOrCreate()
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Cast the columns to doubles
    for col_name in data.columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # Remove any remaining rows with NA values
    # cprint(f"Rows before dropping: {data.count()}")
    # data = data.dropna()
    # cprint(f"Rows after dropping: {data.count()}")

    # Evaluate the model
    model_accuracy = evaluate_lr_model_spark(data)

    return {
        "model": "merge_lr",
        "accuracy": model_accuracy
    }

def evaluate_models_merge_svm(**kwargs):
    """
    Wrapper function to evaluate the SVM model
    """
    # Imports
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql.functions import col

    # Get Data
    data = kwargs['ti'].xcom_pull(task_ids='merge_smoke')

    # Turn data into a Spark DataFrame
    spark = SparkSession.builder.appName("Evaluate_Models_Merge_SVM").getOrCreate()
    schema = StructType([StructField(col_name, StringType(), True) for col_name in data.columns])
    data = spark.createDataFrame(data, schema=schema)

    # Cast the columns to doubles
    for col_name in data.columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # Remove any remaining rows with NA values
    # cprint(f"Rows before dropping: {data.count()}")
    # data = data.dropna()
    # cprint(f"Rows after dropping: {data.count()}")

    # Evaluate the model
    model_accuracy = evaluate_svc_model_spark(data)

    return {
        "model": "merge_svm",
        "accuracy": model_accuracy
    }

########################################################################################
# Final Model Selection
########################################################################################
def select_best_model(**kwargs):
    # Get the model accuracies
    model_accuracies = [
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_p1_lr'),
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_p1_svm'),
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_p2_lr'),
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_p2_svm'),
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_merge_lr'),
        kwargs['ti'].xcom_pull(task_ids='evaluate_models_merge_svm')
    ]

    # Print all the models and their accuracies
    for model in model_accuracies:
        cprint(f"Model: {model['model']} - Accuracy: {model['accuracy']}")

    # Select the largest accuracy (second element in the tuple)
    best_model = max(model_accuracies, key=lambda x: x['accuracy'])

    # Print the best model
    cprint(f"The best model is: {best_model['model']} with an accuracy of {best_model['accuracy']}")

    # Return the best model
    return best_model


########################################################################################
# Dags
########################################################################################
dag = DAG(
    'path_all_final',
    default_args=DAG_ARGS,
    description='HW4 Final DAG',
    schedule_interval='@daily',
)

read_data = PythonOperator(
    task_id='read_data',
    python_callable=read_data_start,
    provide_context=True,
    dag=dag,
)

feature_engineering_p1_task = PythonOperator(
    task_id='feature_engineering_p1',
    python_callable=feature_engineering_p1,
    provide_context=True,
    dag=dag,
)

initial_impute_p1_task = PythonOperator(
    task_id='initial_impute_p1',
    python_callable=initial_impute_p1,
    provide_context=True,
    dag=dag,
)

initial_impute_p2_task = PythonOperator(
    task_id='initial_impute_p2',
    python_callable=initial_impute_p2,
    provide_context=True,
    dag=dag,
)

feature_engineering_p2_task = PythonOperator(
    task_id='feature_engineering_p2',
    python_callable=feature_engineering_p2,
    provide_context=True,
    dag=dag,
)

scrape_smoke_task = PythonOperator(
    task_id='scrape_smoke',
    python_callable=scrape_smoke,
    provide_context=True,
    dag=dag,
)

merge_smoke_task = PythonOperator(
    task_id='merge_smoke',
    python_callable=merge_smoke,
    provide_context=True,
    dag=dag,
)

evaluate_models_p1_lr_task = PythonOperator(
    task_id='evaluate_models_p1_lr',
    python_callable=evaluate_models_p1_lr,
    provide_context=True,
    dag=dag,
)

evaluate_models_p1_svm_task = PythonOperator(
    task_id='evaluate_models_p1_svm',
    python_callable=evaluate_models_p1_svm,
    provide_context=True,
    dag=dag,
)

evaluate_models_p2_lr_task = PythonOperator(
    task_id='evaluate_models_p2_lr',
    python_callable=evaluate_models_p2_lr,
    provide_context=True,
    dag=dag,
)

evaluate_models_p2_svm_task = PythonOperator(
    task_id='evaluate_models_p2_svm',
    python_callable=evaluate_models_p2_svm,
    provide_context=True,
    dag=dag,
)

evaluate_models_merge_lr_task = PythonOperator(
    task_id='evaluate_models_merge_lr',
    python_callable=evaluate_models_merge_lr,
    provide_context=True,
    dag=dag,
)

evaluate_models_merge_svm_task = PythonOperator(
    task_id='evaluate_models_merge_svm',
    python_callable=evaluate_models_merge_svm,
    provide_context=True,
    dag=dag,
)

select_best_model_task = PythonOperator(
    task_id='select_best_model',
    python_callable=select_best_model,
    provide_context=True,
    dag=dag,
)


########################################################################################
# Dag Dependencies
########################################################################################
read_data >> [initial_impute_p1_task, initial_impute_p2_task]
initial_impute_p1_task >> feature_engineering_p1_task
initial_impute_p2_task >> feature_engineering_p2_task
feature_engineering_p1_task >> [evaluate_models_p1_lr_task, evaluate_models_p1_svm_task, merge_smoke_task]
feature_engineering_p2_task >> [evaluate_models_p2_lr_task, evaluate_models_p2_svm_task, merge_smoke_task]
[scrape_smoke_task, feature_engineering_p2_task, feature_engineering_p1_task] >> merge_smoke_task
merge_smoke_task >> [evaluate_models_merge_lr_task, evaluate_models_merge_svm_task]
[evaluate_models_p1_lr_task, evaluate_models_p1_svm_task, evaluate_models_p2_lr_task, evaluate_models_p2_svm_task, evaluate_models_merge_lr_task, evaluate_models_merge_svm_task] >> select_best_model_task


