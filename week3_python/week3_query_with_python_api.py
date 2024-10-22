import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

handle="ccook"
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
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

### Questions

# Remember, this week we are using the Spark DataFrame API (and last week was the Spark SQL API).

#Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.
#You will use the "reviews" dataframe defined here to answer all the questions below...

print("\n\n--- Question 1---\n\n")

reviews_data = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)

#Question 2: Display the schema of the dataframe.

print("\n\n--- Question 2---\n\n")

reviews_data.printSchema()

#Question 3: How many records are in the dataframe? 
#Store this number in a variable named "reviews_count".

print("\n\n--- Question 3---\n\n")

reviews_count = reviews_data.count()
print(reviews_count)

#Question 4: Print the first 5 rows of the dataframe. 
#Some of the columns are long - print the entire record, regardless of length.

print("\n\n--- Question 4---\n\n")

reviews_data.show(n=5, truncate=False)

#Question 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
#Look at the first 50 rows of that dataframe. 
#Which value appears to be the most common?

print("\n\n--- Question 5---\n\n")

product_categories = reviews_data.select("product_category")
product_categories.show(50)

most_common_category = (
    product_categories.groupBy("product_category")
    .count()
    .orderBy("count", ascending=False)
)
most_common_category.show(1)

#Question 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
#What is the product title for that review? How many helpful votes did it have?

print("\n\n--- Question 6---\n\n")

most_helpful_review = reviews_data.sort(reviews_data.helpful_votes.desc())
most_helpful_review.show(1)

#Question 7: How many reviews have a 5 star rating?

print("\n\n--- Question 7---\n\n")

five_star_reviews = (
    reviews_data.groupBy("star_rating")
    .count()
    .orderBy("count", ascending=False)
)
five_star_reviews.show(1)

#Question 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
#Create a new dataframe with just those 3 columns, except cast them as "int"s.
#Look at 10 rows from this dataframe.

print("\n\n--- Question 8---\n\n")

cast_to_int = reviews_data.select(
    reviews_data.star_rating.cast('int'),
    reviews_data.helpful_votes.cast('int'),
    reviews_data.total_votes.cast('int')
)
cast_to_int.show(10)

#Question 8: Find the date with the most purchases.
#Print the date and total count of the date with the most purchases

print("\n\n--- Question 8 #2---\n\n")

date_of_purchase = (
    reviews_data.groupBy("purchase_date")
    .count()
    .orderBy("count", ascending=False)
)
date_of_purchase.show(1)

#Question 9: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
#Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/3.1.3/api/python/reference/pyspark.sql.html#functions
#Print the schema and inspect a few rows of data to make sure the data is correctly populated.
print("\n\n--- Question 9---\n\n")

reviews_timestamp = reviews_data.withColumn("review_timestamp", current_timestamp()) # https://www.educative.io/answers/how-to-add-a-current-timestamp-column-to-pyspark-dataframe
reviews_timestamp.show(5)

#Question 10: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
print("\n\n--- Question 10---\n\n")

filepath = "s3a://hwe-fall-2024/ccook/bronze/reviews_static"
reviews_timestamp.write.parquet(filepath, mode="overwrite")

# try:
    # response = s3.get_object(Bucket="hwe-fall-2024", Key="bronze/reviews_static") # look into s3 api for this, use this instead of get object, prefix and check if empty - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
#     content = response['Body'].read().decode('utf-8')
#     print(content)
# except Exception as e:
#     print(f"Error: {str(e)}")

#Question 11: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
#Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
#There are no questions to answer about this data set right now, but you will use it in a later lab...
print("\n\n--- Question 11---\n\n")

customer_data = spark.read.csv("resources/customers.tsv.gz", sep="\t", header=True)
filepath2 = "s3a://hwe-fall-2024/ccook/bronze/customers"
customer_data.write.parquet(filepath2, mode="overwrite")

# try:
#     response = s3.get_object(Bucket="hwe-fall-2024", Key="bronze/customers")
#     content = response['Body'].read().decode('utf-8')
#     print(content)
# except Exception as e:
#     print(f"Error: {str(e)}")


# Stop the SparkSession
print("\n \n --- End of Program --- \n \n")
spark.stop()