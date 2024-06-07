def start_session():
    """
    Function to create a session with the RDS
    """
    # Wait until lock is released
    cprint("Waiting for the lock to be released")
    time.sleep(10)
    cprint("Starting a new session")
    # Create a connection to the RDS
    engine = create_engine(CONN_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    cprint("Session created successfully")
    return session

def write_data_to_db(data: pd.DataFrame, table_name: str, session: Session) -> None:
    """
    Function to write data to the RDS.
    """
    if not session.is_active:
        cprint("Session is not active, restarting session")
        session = start_session()  # Ensure start_session returns an active session
    else:
        cprint("Session is active")

    try:
        cprint("Transaction started")

        # Check if table exists
        exists = session.execute(f"SELECT to_regclass('{table_name}')").scalar() is not None
        if exists:
            cprint(f"Table '{table_name}' already exists, dropping table")
            session.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create a new table and write the data
        cprint("Writing data to the database")
        data.to_sql(table_name, session.connection(), if_exists='replace', index=False)
        
        cprint("Data written to database successfully")
        
        # Commit the transaction
        session.commit()

    except SQLAlchemyError as e:
        # Automatic rollback on exception
        cprint(f"An error occurred: {e}")
    finally:
        session.close()
        cprint("Session closed")


# RDS, host for local, endpoint for AWS
CONN_URI = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['default_db']}"
print(f"RDS Connection URI: {CONN_URI}")




########################################################################################
# HELPER FUNCTIONS FOR P2
########################################################################################

################################# P2 Specific Constants ################################
imputers = []
imputed_columns = []
cols = DATA_COLS

def p2_s3_pk(file_key: str) -> pd.DataFrame:
    """
    Function to construct the S3 path and key for the given file key
    """
    return f"{P2_PATH}/{file_key}"

def fetch_and_write_data_db_spark(file_key):
    """
    Decorator function to fetch the data from S3
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Start the Spark session
            spark = SparkSession.builder.appName("HeartDisease").getOrCreate()
            data = spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f"s3a://{PARAMS['s3']['bucket']}/{file_key}")

            # Call the function
            new_data = func(spark, data)
            target_key = new_data['target']

            # Number of NA rows
            na_row_count = new_data['data'].isna().any(axis=1).sum()
            cprint(f"Number of rows containing at least one NA value: {na_row_count}")

            # Save data back in S3 by creating an object, key path = P1_PATH + target_key
            # Convert the data to a Pandas DataFrame and write to a CSV file
            df = data.toPandas()
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            S3C.put_object(Bucket=PARAMS['s3']['bucket'], Key=target_key, Body=csv_buffer.getvalue())

            cprint("Data written to S3 successfully")
            cprint(new_data['data'].head())

            # Return the data
            return new_data
        return wrapper
    return decorator

################################### SPARK HELPER FUNCTIONS ###################################
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

################################### SPARK MAIN FUNCTIONS ###################################
@fetch_and_write_data_db_spark(PARAMS['s3']['data_key'])
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
    return {
        "data": data,
        "target": p2_s3_pk(P2_FN1)
    }

@fetch_and_write_data_db_spark(p2_s3_pk(P2_FN1))
def feature_engineering_p2(spark: SparkSession, data: DataFrame):
    # Clean the data further using techniques like imputation and one-hot encoding
    # 1. Replace painloc and painexer NAs with 0 (default value)
    data = data.withColumn("painloc", coalesce(col("painloc"), lit(0)))
    data = data.withColumn("painexer", coalesce(col("painexer"), lit(0)))

    # 2. CP is a categorical variable, remove all NA rows
    data = data.na.drop(subset=["cp"])

    # 3. Trestbps is a continuous variable, replace NAs and <= 100 with's the mode
    data = fill_with_mode(data, "trestbps")
    data = data.withColumn("trestbps", when(col("trestbps") <= 100, 130).otherwise(col("trestbps")))

    # 4. Clean the smoke column with mode for P2
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

    return {
        "data": data,
        "target": p2_s3_pk(P2_FN2)
    } 

@fetch_and_write_data_db_spark(p2_s3_pk(P2_FN2))
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
            .addGrid(classifiers[1].maxIter, [20, 40]) \
            .addGrid(classifiers[1].regParam, [0.01, 0.1]) \
            .addGrid(classifiers[1].elasticNetParam, [0.0, 0.5]) \
            .addGrid(classifiers[1].tol, [1e-6, 1e-4]) \
            .build(),
        ParamGridBuilder() \
            .addGrid(classifiers[0].maxIter, [20, 40]) \
            .addGrid(classifiers[0].regParam, [0.01, 0.1]) \
            .addGrid(classifiers[0].tol, [1e-6, 1e-4]) \
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
    
    # Return the model accuracies
    return {
        "data": model_accuracies_df,
        "target": p2_s3_pk(P2_FN3)
    }

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

    # Return the model name and accuracy
    return model_name, auc