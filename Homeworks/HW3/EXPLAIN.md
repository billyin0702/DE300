# Homework 3 Readme
All relevant code will be inside [hw3_process.py](hw3_process.py), from data reading to data cleaning. The logic for cleaning will be the same as Homework 2, they will be listed below.

# 1. Data Cleaning
## 1.1 Understanding the columns
Each column in the dataset is describe below with their respective information:

| Variable  | Description                                         | Values                                                         |
|-----------|-----------------------------------------------------|----------------------------------------------------------------|
| age       | Age of the patient                                  | age in years                                                   |
| sex       | Sex of the patient                                  | 1: male; 0: female                                             |
| painloc   | Chest pain location                                 | 1: substernal, 0: otherwise                                    |
| painexer  | Chest pain during exercise                          | 1: provoked by external exertion, 0: otherwise                 |
| cp        | Chest pain type                                     | 1: typical angina, 2: atypical angina, 3: non-anginal pain, 4: asymptomatic |
| trestbps  | Resting blood pressure                              | mmHg                                                           |
| smoke     | Smoking status                                      | 1: smoker, 0: otherwise                                        |
| fbs       | Fasting blood sugar > 120 mg/dl                     | 1: true, 0: false                                              |
| prop      | Beta blocker used during exercise ECG               | 1: used, 0: not used                                           |
| nitr      | Nitrates used during exercise ECG                   | 1: used, 0: not used                                           |
| pro       | Calcium channel blocker used during exercise ECG    | 1: used, 0: not used                                           |
| diuretic  | Diuretic used during exercise ECG                   | 1: used, 0: not used                                           |
| thaldur   | Duration of exercise test                           | minutes                                                        |
| thalach   | Maximum heart rate achieved                         | bpm                                                            |
| exang     | Exercise induced angina                             | 1: yes, 0: no                                                  |
| oldpeak   | ST depression induced by exercise relative to rest  | mm                                                             |
| slope     | Slope of the peak exercise ST segment               | 1: upsloping, 2: flat, 3: downsloping                          |
| target    | Diagnosis of heart disease                          | 1: heart disease, 0: no heart disease                          |


## 1.2 Steps Taken
All cleaning steps are the same as Homework 2, but they are implemented using PySpark instead, which means some parts have to change:
1. Filling with mode is now done with the ```fill_with_mode``` function, and casted to a PySpark ```udf``` function
2. Filling with mean is now done with the ```fill_with_mean``` function, and casted to a PySpark ```udf``` function
3. ```clean_smoke``` has now been separated into the initial cleaning state. The reason for this is because there are conflicting package imports
between PySpark and Scrapy, which is what we used in HW2 to scrape the smoke data. Therefore the data cleaning and modeling procedures is as detailed below.

### 1.2.1 What does ```hw3_step_1.py``` do?
1. Read the heart disease dataset
2. Clean the dataset initially
3. Clean the smoke column
4. Write the cleaned data to a directory

### 1.2.2 What does ```hw3_step_2.py``` do?
1. Read the cleaned data from Step 1
2. Clean the data further using techniques like imputation and one-hot encoding 
3. Evaluate the models using the cleaned data

To construct the pipeline in Part 2, some of the columns that can be imputed automatically (NAs to some number) without a specified condition (e.g. between certain values), are packaged as imputers to become part of the pipeline later. The columns that can be imputed automatically are:
- thaldur
- thalach
- slope

# 2. Data Modeling
## 2.1 Steps Taken
A lot of the data modeling steps are taken from Lab 7. The steps are as follows:
1. Split the data into training and testing sets (90/10)
2. Create a pipeline with the following stages:
    - Imputation for numerical columns
    - Vector assembler to combine all features into a single vector
    - Model training (Logistic Regression, Random Forest, Gradient Boosted Trees...)
3. Evaluate the models using the testing set and find the best model

## 2.2 Model Evaluation
After running the models, the following results were obtained:

### 2.1 Random Forest
| Metric                    | Value  |
|---------------------------|--------|
| Area Under ROC Curve      | 0.8474 |
| Best maxDepth             | 4      |
| Best numTrees             | 1000   |
| Best maxBins              | 50     |
| Best featureSubsetStrategy| sqrt   |
| Time Taken (seconds)      | 194.29 |

### 2.2 Logistic Regression
| Metric                  | Value  |
|-------------------------|--------|
| Area Under ROC Curve    | 0.8599 |
| Best maxIter            | 20     |
| Best regParam           | 0.01   |
| Best elasticNetParam    | 0.0    |
| Best tol                | 1e-06  |
| Time Taken (seconds)    | 36.77  |

### 2.3 Gradient Boosted Trees Classifier
| Metric                  | Value   |
|-------------------------|---------|
| Area Under ROC Curve    | 0.8059  |
| Best maxDepth           | 4       |
| Best maxIter            | 20      |
| Best maxBins            | 40      |
| Best stepSize           | 0.1     |
| Time Taken (seconds)    | 263.70  |

### 2.4 Decision Tree Classifier
| Metric                     | Value  |
|----------------------------|--------|
| Area Under ROC Curve       | 0.7704 |
| Best maxDepth              | 4      |
| Best maxBins               | 20     |
| Best minInfoGain           | 0.1    |
| Best minInstancesPerNode   | 1      |
| Time Taken (seconds)       | -      |


## 2.3 Conclusion
From the models tested, Logistic Regression performed the best with an AUC of 0.8599, with Random Forest coming in second with an AUC of 0.8474. The Gradient Boosted Trees Classifier and Decision Tree Classifier performed the worst with AUCs of 0.8059 and 0.7704 respectively.

This can be attributed to the lack of overfitting from Random Forest but it seems that the Logistic Regression model was able to generalize the data better.