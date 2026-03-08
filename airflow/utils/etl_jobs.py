from pyspark.sql import SparkSession

app_name = "nyc_taxi_query"

spark = None

try:
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse_prod", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse_prod.type", "hive")
        .config("spark.sql.catalog.lakehouse_prod.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse_prod.warehouse", "s3a://lakehouse-prod-bucket/warehouse/")
        .config("spark.sql.catalog.lakehouse_prod.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.catalog.lakehouse_prod.s3.region", "us-east-1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession created successfully!")

    spark.sql("USE lakehouse_prod").show()
    spark.sql("SHOW CATALOGS;").show()
    csv_path = "s3a://datalake-landing/breweries.csv"
    df = spark.read.option("header", "true").csv(csv_path)
    df.show(10)

    df.writeTo("lakehouse_prod.nyc.breweries") \
        .tableProperty("location", "s3a://lakehouse-dev-bucket/external_test/breweries") \
        .createOrReplace()

    df = spark.read.format("iceberg").load("lakehouse_prod.nyc.breweries")
    df.show()
    print("Query executed successfully!")

except Exception as e:
    print(f"Error: {e}")

finally:
    if spark:
        spark.stop()
        print("Spark stopped.")