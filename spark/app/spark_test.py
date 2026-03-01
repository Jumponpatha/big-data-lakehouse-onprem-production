from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder.appName("SparkTest").getOrCreate()
    print("SparkSession created successfully.")
except Exception as e:
    print(f"Error creating SparkSession: {e}")

try:
    sc = spark.sparkContext
    print("SparkContext created successfully.")
except Exception as e:
    print(f"Error creating SparkContext: {e}")


spark.sql("""
    SELECT * FROM nyc.yellow_trip_data_202204
""").show(truncate=False)

spark.stop()