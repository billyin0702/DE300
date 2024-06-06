from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine

# Helper functions for HW2
import requests
import pandas as pd
import numpy as np
import re

from scrapy import Selector
from typing import List
from pandas import DataFrame

from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
from sklearn.model_selection import cross_val_predict, GridSearchCV


def load_web_selector(url: str) -> Selector:
    """
    Load a webpage and return a Selector object for parsing
    """
    # Load the webpage
    r = requests.get(url)
    r.raise_for_status()

    # Create the Selector object
    return Selector(text=r.content)


def find_best_model(clf, param_dist, X_train, y_train, X_test, y_test):
    """
    Find the best model using GridSearchCV
    """
    # Create a GridSearchCV object
    grid_search = GridSearchCV(estimator=clf, param_grid=param_dist, cv=5, scoring='accuracy') # Can turn on verbose to see progress
    grid_search.fit(X_train, y_train)

    # Print the tuned parameters and score
    print("Best Parameters: {}".format(grid_search.best_params_))
    print("Best Score is {}".format(grid_search.best_score_))

    # Retrieve the best model
    best_clf_rf = grid_search.best_estimator_
    print("########################## Cross-Validation Evaluation ##########################")
    five_fold_cross_validation(best_clf_rf, X_train, y_train)

    # Evaluate the best model on the test set
    y_pred = best_clf_rf.predict(X_test)
    print("########################## Test Set Evaluation ##########################")
    print("Test Set Accuracy:", accuracy_score(y_test, y_pred))
    evaluate_classifier(y_test, y_pred)


def five_fold_cross_validation(classifier, X_train, y_train):
    """
    Perform 5-fold cross-validation for a classifier
    """

    # Perform 5-fold cross-validation
    y_pred = cross_val_predict(classifier, X_train, y_train, cv=5)

    # Evaluate the classifier
    print("Cross-Validation Accuracy:", accuracy_score(y_train, y_pred))
    evaluate_classifier(y_train, y_pred)

def evaluate_classifier(y_true: List[str], y_pred: List[str]):
    """
    Evaluate a classifier and print the confusion matrix and classification report
    """
    conf_mat_orig = confusion_matrix(y_true, y_pred)
    
    # Turn confusino matrix into a DataFrame and percentages
    conf_mat_per = pd.DataFrame(conf_mat_orig, index=["Actual 0", "Actual 1"], columns=["Predicted 0", "Predicted 1"])
    conf_mat_per = conf_mat_per / conf_mat_per.sum().sum() * 100

    # Print the confusion matrix
    print("Confusion Matrix:")
    print(conf_mat_orig)

    print("Confusion Matrix (as percentages):")
    print(conf_mat_per)

    # Print the classification report
    print("\nClassification Report:")
    print(classification_report(y_true, y_pred))

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

def smoke_assign(data_len: int, prob: float) -> int:
    return np.random.choice([1, 0], size=data_len, p=[prob, 1 - prob])

def clean_smoke(data: DataFrame) -> DataFrame:
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
    abs_data = pd.DataFrame(abs_rows, columns=abs_header)[["Age Group", "2022 (%)"]]

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
    cdc_data["ages"][(65, float('inf'))] = extract_percentage(cdc_age[3])

    # Alter data to be the following:
    # Female (0) - Corresponding age group percentage
    # Male   (1) - Corresponding age group percentage * smoking rate among men / smoking rate among women
    cdc_alt_data = {}
    cdc_alt_data[0] = cdc_data['ages']
    cdc_alt_data[1] = {}
    for age_range in cdc_data['ages']:
        cdc_alt_data[1][age_range] = cdc_data['ages'][age_range] * cdc_data['sex']['male'] / cdc_data['sex']['female']

    # Add new smoke columns to the data based on the extracted data
    # Australian Bureau of Statistics Data
    for index, row in abs_data.iterrows():
        age_group = row['Age Group']
        ages = extract_numbers(age_group)
        prob = float(row['2022 (%)'])/100

        # Assign the data based on a percentage probability
        if len(ages) == 2:
            data_len = len(data[(data['age'] >= ages[0]) & (data['age'] <= ages[1])])
            data.loc[(data['age'] >= ages[0]) & (data['age'] <= ages[1]), 'smoke_abs'] = smoke_assign(data_len, prob)
        elif len(ages) == 1:
            data_len = len(data[data['age'] >= ages[0]])
            data.loc[data['age'] >= ages[0], 'smoke_abs'] = smoke_assign(data_len, prob)

    # CDC Data, follow the same procedure as above
    # Female (0) - Corresponding age group percentage
    # Male   (1) - Corresponding age group percentage * smoking rate among men / smoking rate among women

    for gender in cdc_alt_data:
        for age_range in cdc_alt_data[gender]:
            prob = cdc_alt_data[gender][age_range]/100 # Convert to percentage

            bool_mask = (data['age'] >= age_range[0]) & (data['age'] <= age_range[1]) & data['sex'] == gender
            data_len = len(data[bool_mask])
            data.loc[bool_mask, 'smoke_cdc'] = smoke_assign(data_len, prob)

    # Aggergate all 3 columns, if any of them are 1, then the person smokes
    # Do so only for rows that have NA values in the smoke column
    data['smoke'] = data[['smoke_abs', 'smoke_cdc']].apply(lambda x: 1 if 1 in x.values else 0, axis=1)

    # Drop auxiliary columns
    data = data.drop(columns=['smoke_abs', 'smoke_cdc'])

    return data