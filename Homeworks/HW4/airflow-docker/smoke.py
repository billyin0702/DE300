from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, coalesce, lit, mean, udf
from pyspark.sql.types import IntegerType, ArrayType, BooleanType

import os
import boto3
import pandas as pd
import tomli
from typing import List
import requests

import re
import numpy as np
import os


from scrapy import Selector
from typing import List

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
aws_access_key_id="ASIAYAAO5HRMHZOKAUBH"
aws_secret_access_key="PGAjoX5Xj1cwk9zPZKv6qesvp9XNLlcN8SogqFEA"
aws_session_token="IQoJb3JpZ2luX2VjEFkaCXVzLWVhc3QtMiJIMEYCIQDlEJgNvuGqPHp48Q1UCQKwcuD2WwL2bqQnYbLNOb540wIhAMIqmIp76x+K/fSHsQutdW7kr1yv9fP9DuWsf6wc4G+3KvQCCOL//////////wEQABoMNTQ5Nzg3MDkwMDA4IgwOYy/5ahxmwpYuorkqyALO46ItILkq6zAb6j9Z8zIW33Iz0jxj/NAaKQoYycspV5wfEDGV6StZEJxjE7+bW9oVO3+fjyqcD6Uh+mJYVahhcjubU40XI9Mz/tZFjRdWYhMMAxc7zORurWoMdORsI2zXsaCMZgkhcq0C/Z4X72x3Wuis59IiwEBBsCPeARHnvUG5p8me41eP2kScZP7JyRWwjqyaW35hu0wK/FyNcfEnEAj0VAojVeIvEetTt8Cpxfl6r/JMFcbz90WfAbdKlppndlKpHqlWZVbKTK00FnPRRcCuJ2iyLcuvRGPdPeqUC02BSUnJfWRoMzzmLvSUR/whNG974cX6ul0V4SJXf0FyaTnxxRyPG8xe1vuXkZshkHWXU0P4xiWrFzTnBP0f/NdT2kLUHXC4KxtGt7mlMQZ2l+WThiQUePFF7B8bc6sjKVXx6ISQwo/NMLuxibMGOqYB3IJG1apd6M/m56JyhLGxWEEUK0pXhiSS4LD7kFmKybEBmHv+awz8VGNvLo9h/zXMDPAhWvJn0MW1g76Z0nqaIA4lfwyv6M9oDG7QpvdyZGVEAEBtm4GBtb8lgL7CTk4VY9dBzxkSfXRw72CqpglmPbbHS1XxxS3vcUmKXrOcwkY3MC/L7fVI+ncLtnCcvuooN6mmHaeW8vEGZWi0Pt9efID+aFIoTQ=="

S3C = boto3.client('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token)
# # On AWS
# S3C = boto3.client('s3')

MAX_AGE = 150

######################################
# Overview
######################################

# What this file does:
# 1. Clean the 'smoke' column in the heart disease dataset

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