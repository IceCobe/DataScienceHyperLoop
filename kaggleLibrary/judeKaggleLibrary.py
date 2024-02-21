from pyspark.sql import SparkSession
import pandas as pd

def load_data(file_name):
    # Initialize a SparkSession
    spark = SparkSession.builder.appName('KaggleData').getOrCreate()

    # Read the CSV data into a DataFrame
    train_df = spark.read.csv(f'../data/{file_name}/train.csv', header=True, inferSchema=True)
    test_df = spark.read.csv(f'../data/{file_name}/test.csv', header=True, inferSchema=True)

    return train_df, test_df

def create_csv(file_name, predictions, cols=["id", "prediction"]):
    # Assuming 'df' is your Spark DataFrame
    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = predictions.select(cols[0], cols[1]).toPandas()

    # Define the local path where you want to save the CSV
    output_csv_path = f"../data/{file_name}/submission.csv"

    # Save the Pandas DataFrame as a CSV file
    pandas_df.to_csv(output_csv_path, index=False)