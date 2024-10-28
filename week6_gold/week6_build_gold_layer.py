import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week6Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,io.delta:delta-core_2.12:1.0.1') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

#Define a Schema which describes the Parquet files under the silver reviews directory on S3
silver_schema = StructType([
   StructField("marketplace", StringType(), nullable=False)
  ,StructField("customer_id", StringType(), nullable=False) 
  ,StructField("product_id", StringType(), nullable=False) 
  ,StructField("product_parent", StringType(), nullable=False)
  ,StructField("product_title", StringType(), nullable=False)
  ,StructField("product_category", StringType(), nullable=False)
  ,StructField("star_rating", IntegerType(), nullable=False)
  ,StructField("helpful_votes", IntegerType(), nullable=False)
  ,StructField("total_votes", IntegerType(), nullable=False)
  ,StructField("vine", StringType(), nullable=False)
  ,StructField("verified_purchase", StringType(), nullable=False) 
  ,StructField("review_headline", StringType(), nullable=False)
  ,StructField("review_body", StringType(), nullable=False)
  ,StructField("purchase_date", StringType(), nullable=False) 
  ,StructField("review_timestamp", TimestampType(), nullable=False) 
  ,StructField("customer_name", StringType(), nullable=False)
  ,StructField("gender", StringType(), nullable=False) 
  ,StructField("date_of_birth", StringType(), nullable=False)
  ,StructField("city", StringType(), nullable=False)
  ,StructField("state", StringType(), nullable=False) 
  ])

#Define a streaming dataframe using readStream on top of the silver reviews directory on S3
silver_data = spark.readStream \
.format("parquet") \
.schema(silver_schema) \
.load("s3a://hwe-fall-2024/ccook/silver/reviews/")

#Define a watermarked_data dataframe by defining a watermark on the `review_timestamp` column with an interval of 10 seconds
watermarked_data = silver_data.withWatermark("review_timestamp", "10 seconds")

#Define an aggregated dataframe using `groupBy` functionality to summarize that data over any dimensions you may find interesting
aggregated_data = watermarked_data.groupBy()

#Write that aggregate data to S3 under s3a://hwe-$CLASS/$HANDLE/gold/fact_review using append mode and a checkpoint location of `/tmp/gold-checkpoint`
write_gold_query = aggregated_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://hwe-fall-2024/ccook/gold/fact_review") \
    .option("checkpointLocation", "/tmp/gold-checkpoint") \
    .start()

write_gold_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()