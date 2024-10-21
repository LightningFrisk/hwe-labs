import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# 1. Define a `bronze_schema` which describes the Parquet files under the bronze reviews directory on S3

# Uses schema inference
# spark.read.csv("path")

# Uses a custom schema
# custom_schema = …
# spark.read.schema(custom_schema).csv("path")

#  Manually define a schema that matches your data
#     from pyspark.sql.types import StructType, StructField, StringType
# player_schema = StructType([
#    StructField("name", StringType, nullable=false)
#   ,StructField("gender", StringType, nullable=false)
#   ,StructField("state", StringType, nullable=false)])
# players = spark.read.schema(player_schema).csv("path")

# Can also nest struct fields together!
# nested_schema = StructType([
#   player_schema
#  ,StructField("score", IntegerType, nullable=false)])

bronze_schema = StructType([
   StructField("marketplace", StringType, nullable=false)
  ,StructField("customer_id", StringType, nullable=false) #could be int
  ,StructField("product_id", StringType, nullable=false) #could be int
  ,StructField("product_parent", StringType, nullable=false)
  ,StructField("product_title", StringType, nullable=false)
  ,StructField("product_category", StringType, nullable=false)
  ,StructField("star_rating", IntegerType, nullable=false)
  ,StructField("helpful_votes", IntegerType, nullable=false)
  ,StructField("total_votes", IntegerType, nullable=false)
  ,StructField("vine", StringType, nullable=false)
  ,StructField("verfied_purchase", StringType, nullable=false) #could be boolean
  ,StructField("review_headline", StringType, nullable=false)
  ,StructField("review_body", StringType, nullable=false)
  ,StructField("purchase_date", StringType, nullable=false) #could be date
  ])

# 2. TODO Define a streaming dataframe using `readStream` on top of the bronze reviews directory on S3
    # df = spark.readStream.parquet("path")

    # df = spark \ This won't work, but gives framework on how to do this,but this is kafka so maybe too complicated, i think reading from s3 is just a url
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    #     .option("subscribe", kafka_topic) \
    #     .option("startingOffsets", "earliest") \
    #     .option("maxOffsetsPerTrigger", "1000") \
    #     .option("kafka.security.protocol", "SASL_SSL") \
    #     .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    #     .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    #     .load()

# 3. TODO Register a virtual view on top of that dataframe

bronze_reviews = None

# TODO 4. Define a non-streaming dataframe using `read` on top of the bronze customers directory on S3
# TODO 5. Register a virtual view on top of that dataframe

bronze_customers = None

# TODO 6. Define a `silver_data` dataframe by:
#    * joining the review and customer data on their common key of `customer_id`
#    * applying a business validation rule to prevent unverified reviews in the bronze layer from being loaded into the silver layer

silver_data = None

# 7. Write that silver data to S3 under `s3a://hwe-$CLASS/$HANDLE/silver/reviews` using append mode, a checkpoint location of `/tmp/silver-checkpoint`, and a format of `parquet`

# streaming_query = silver_data.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "s3a://hwe-fall-2024/ccook/silver/reviews") \
#     .option("checkpointLocation", "/tmp/silver-checkpoint") \
#     .start()

print("\n\n--- Write Complete ---\n\n")

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
