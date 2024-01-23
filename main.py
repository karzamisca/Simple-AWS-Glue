import random
import string

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when

# Initialize contexts
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Read data from an existing CSV file in S3
input_s3_path = "s3://mqawsbucket/test/test1.csv"
source_data = spark.read.csv(input_s3_path, header=True, inferSchema=True)

# Generate a random string for the folder name
random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=8))  # Adjust the length as needed

# Define the S3 output path with the random string
output_s3_path = f"s3://mqawsbucket/shop/{random_string}/"

# Apply transformations
transformed_data = source_data.withColumn("field 4",
                                         concat(col("field 4"), lit("_"), col("field 6"), lit("_"), col("field 7"))) \
    .drop("field 6", "field 7", "field 8", "field 9") \
    .withColumn("field 5", when(col("Condition") == 'Condition1', None).otherwise(col("field 5"))) \
    .select("field 1", "field 2", "field 3", "field 4", "field 5")

# Repartition DataFrame to a single partition
transformed_data = transformed_data.coalesce(1)

# Write transformed data to the randomized folder in S3 with a single partition
transformed_data.write.csv(output_s3_path, header=True)
