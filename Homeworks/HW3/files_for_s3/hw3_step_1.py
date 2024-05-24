# File for performing HW2 processes but on Spark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import col, when, udf

import requests
import re
import numpy as np
import os

from scrapy import Selector
from typing import List

######################################
# Overview
######################################

# What this file does:
# 1. Read the heart disease dataset
# 2. Clean the dataset initially
# 3. Clean the smoke column
# 4. Write the cleaned data to a directory

# ######################################
# # Constants
# ######################################

# ROOT_PATH = "data"
ROOT_PATH = "s3://de300spring2024/bill_yin/hw3_spark/actual_homework/data/"
DATA_PATH = "heart_disease.csv"
WRITE_PART_PATH = "hd_cleaned/"
OUTPUT_PATH = "hd_cleaned.csv"

MAX_AGE = 150
TRAIN_TEST_SPLIT = 0.9
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
    if len(rg) == 1:
        return age >= rg[0]
    return rg[0] <= age <= rg[1]

def same_gender(g1: int, g2: int):
    return int(g1) == int(g2)

######################################
# SPARK HELPER FUNCTIONS
######################################
def read_data(spark: SparkSession):
    """
    Read the data
    """
    # Read the dataset from a CSV file, but remove the c0_ prefix from the column names
    data = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(os.path.join(ROOT_PATH, DATA_PATH))
    
    return data

def write_data(data):
    """
    Write the data to a directory
    """
    # Write the data partitinos to a directory
    data.write.csv(os.path.join(ROOT_PATH, WRITE_PART_PATH), mode="overwrite")
    cprint("Wrote all partitions to a directory")
    # Convert the data to a Pandas DataFrame and write to a CSV file
    df = data.toPandas()
    df.to_csv(os.path.join(ROOT_PATH, OUTPUT_PATH), index=False)
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

######################################
# SPARK MAIN FUNCTIONS
######################################

def clean_data_initial(data: DataFrame):
    """
    Clean the data
    """
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
    return data

def clean_smoke(spark: SparkSession, data: DataFrame) -> DataFrame:
    """
    Clean the smoke column by removing the 'smoke' prefix
    """

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

    # Createa a dictionary of the ranges
    abs_data = spark.createDataFrame(abs_rows, abs_header)

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

    # CDC Data, follow the same procedure as above
    # Female (0) - Corresponding age group percentage
    # Male   (1) - Corresponding age group percentage * smoking rate among men / smoking rate among women

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

    return data

######################################
# MAIN FUNCTION
######################################

def main():
    cprint("Starting the process...")
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Read Heart Disease Dataset") \
        .getOrCreate()
    
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    cprint("Reading data...")

    # Read the data
    data = read_data(spark)
    cprint("Read data!")

    # Clean the data
    cprint("Cleaning data initially!")
    data = clean_data_initial(data)
    cprint("Cleaned data!")

    # Clean the smoke column    
    cprint("Cleaning smoke column...")
    data = clean_smoke(spark, data)
    cprint("Cleaned smoke column")
    
    # Write the data to a directory
    write_data(data)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()