from pyspark.sql import SparkSession
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sort IoT Data") \
    .master("spark://master:7077") \
    .getOrCreate()

# Input and Output paths from command-line arguments
input_path = sys.argv[1]   # hdfs://nn:9000/input/export.csv
output_path = sys.argv[2]  # hdfs://nn:9000/output/sorted_data

# Read the CSV file from HDFS into a DataFrame
df = spark.read.option("header", "true").csv(input_path)

# Sort the data by 'cca2' (country code) and 'timestamp'
sorted_df = df.orderBy(["cca2", "timestamp"])

# Write the sorted data back to HDFS
sorted_df.write.csv(output_path, header=True)

# Stop the Spark Session
spark.stop()