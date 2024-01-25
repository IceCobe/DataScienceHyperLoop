from pyspark.sql import SparkSession

def load_data(file_name):
    # Initialize a SparkSession
    spark = SparkSession.builder.appName('KaggleData').getOrCreate()

    # Read the CSV data into a DataFrame
    train_df = spark.read.csv(f'../data/{file_name}/train.csv', header=True, inferSchema=True)
    test_df = spark.read.csv(f'../data/{file_name}/test.csv', header=True, inferSchema=True)

    return train_df, test_df