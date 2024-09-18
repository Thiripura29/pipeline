import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from great_expectations.data_context import DataContext
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

# Initialize Spark session
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "python"

spark = SparkSession.builder \
    .appName("GreatExpectationsSparkExample") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0,org.apache.hadoop:hadoop-aws:3.2.1,org.apache.spark:spark-hadoop-cloud_2.13:3.2.1") \
    .getOrCreate()

# Set S3 credentials
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAQKPIMDVLUR3AV7TZ")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

df = spark.read.format("delta").load("s3a://prudhvi-08052024-test/silver/intermediate/loan_approval")
df.printSchema()
# df.where("loan_id in ('5','7','8','9')").show(4,False,True)
# good_records = df.where("dq_validations.run_row_success == true").drop('dq_validations')
# bad_records = df.where("dq_validations.run_row_success == false")
# good_records.show(2, False, True)
# bad_records.show(2, False, True)
# # Create sample data
# data = [
#     {"ID": 1, "Name": "Ramesh", "Age": 25, "Gender": "Male", "Salary": 1000},
#     {"ID": 2, "Name": "Nasser", "Age": 25, "Gender": "Male", "Salary": 2500},
#     {"ID": 3, "Name": "Jessica", "Age": 25, "Gender": "Female", "Salary": 5000},
#     {"ID": 4, "Name": "Komal", "Age": 20, "Gender": "Female", "Salary": 3500},
#     {"ID": 5, "Name": "Jude", "Age": 20, "Gender": "Male", "Salary": 6900},
#     {"ID": 6, "Name": "Muffy", "Age": 25, "Gender": "Female", "Salary": 1200}
# ]
#
# # Create a Spark DataFrame
# df = spark.createDataFrame(data)
#
# # Initialize Great Expectations context
# context = DataContext()
#
# # Define expectation suite name
# suite_name = "example_suite"
#
# # Check if the suite already exists
# existing_suites = [suite.expectation_suite_name for suite in context.list_expectation_suites()]
# if suite_name not in existing_suites:
#     # Create a new expectation suite
#     suite = context.add_expectation_suite(expectation_suite_name=suite_name)
#     print(f"Expectation suite '{suite_name}' created.")
# else:
#     suite = context.get_expectation_suite(expectation_suite_name=suite_name)
#     print(f"Expectation suite '{suite_name}' already exists.")
#
# # Convert Spark DataFrame to Great Expectations SparkDFDataset
# ge_df = SparkDFDataset(df)
#
# # Add expectations to the suite
# ge_df.expect_column_values_to_be_between(
#     column="Salary",
#     min_value=2000,
#     max_value=10000
# )
#
# # Save the expectation suite
# context.save_expectation_suite(expectation_suite=suite)
# print(f"Expectation suite '{suite_name}' saved.")
#
# # Validate the data
# results = ge_df.validate(expectation_suite=suite)
#
# # Print the results
# print("\nValidation Results:")
# print(results)
#
# # Extract and print good and bad records based on validation results
# good_records = df.filter("Salary BETWEEN 2000 AND 10000")
# bad_records = df.filter("Salary NOT BETWEEN 2000 AND 10000")
#
# print("\nGood Records:")
# good_records.show()
#
# print("\nBad Records:")
# bad_records.show()
