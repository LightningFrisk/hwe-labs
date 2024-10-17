import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, split, col

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.ssl.trustStore",'C:\\Users\\bmxca\\.hwe_venv\\Lib\\site-packages\\certifi\\cacert.pem') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load()

# Process the received data

print("\n\n--- Begin Program --- \n\n")

# Modify the `df` dataframe defined in the lab to do the following:
#    * split the value of the Kafka message on tab characters, assigning a field name to each element using the `as` keyword

df.printSchema()
df = df.selectExpr("split(value, '\t')[0] AS marketplace"
               ,"split(value, '\t')[1] AS customer_id"
               ,"split(value, '\t')[2] AS review_id"
               ,"split(value, '\t')[3] AS product_id"
               ,"split(value, '\t')[4] AS product_parent"
               ,"split(value, '\t')[5] AS product_title"
               ,"split(value, '\t')[6] AS product_category"
               ,"cast(split(value, '\t')[7] as int) AS star_rating"
               ,"cast(split(value, '\t')[8] as int) AS helpful_votes"
               ,"cast(split(value, '\t')[9] as int) AS total_votes"
               ,"split(value, '\t')[10] AS vine"
               ,"split(value, '\t')[11] AS verified_purchase"
               ,"split(value, '\t')[12] AS review_headline"
               ,"split(value, '\t')[13] AS review_body"
               ,"split(value, '\t')[14] AS purchase_date") \
                .withColumn("review_timestamp", current_timestamp())
            
#    * append a column to the data named `review_timestamp` which is set to the current_timestamp



#    * write that data as Parquet files to S3 under `s3a://hwe-$CLASS/$HANDLE/bronze/reviews` using append mode and a checkpoint location of `/tmp/kafka-checkpoint`
   
print("\n\n--- Data Frame Loaded ---\n\n")

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://hwe-fall-2024/ccook/bronze/reviews") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()

print("\n\n--- Write Complete ---\n\n")

# Outside of this program, create a table on top of your S3 data in Athena, and run some queries against your data to validate it is coming across the way you expect. Some useful fields to validate could include:

#    * product_title
#    * star_rating
#    * review_timestamp

# GROUP BY and LIMIT are also useful here.

# query = None

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession

print("\n\n--- End of Program ---\n\n")
spark.stop()