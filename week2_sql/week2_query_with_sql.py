import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

### Setup: Create a SparkSession
spark = SparkSession.builder \
        .appName("reviewsSession") \
        .master("local") \
        .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".

reviews_data = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.

reviews_data.createOrReplaceTempView("reviews")

# TODO Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 

print("\n\n--- Question 3---\n\n")

# reviews_data_with_timestamp = spark.sql(
#     "ALTER TABLE reviews ADD COLUMN review_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP"
# ) # this throws an error, not sure why?
# reviews_data_with_timestamp.show(1) 

## TODO Can I do this with SQL like above? 

reviews_data_with_timestamp = reviews_data.withColumn("current_timestamp", current_timestamp()) # https://www.educative.io/answers/how-to-add-a-current-timestamp-column-to-pyspark-dataframe
reviews_data_with_timestamp.show(1) 

# Question 4: How many records are in the reviews dataframe? 

print("\n\n--- Question 4---\n\n")
total_reviews = spark.sql("SELECT COUNT(*) FROM reviews")

total_reviews.show() #this works


# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.


print("\n\n--- Question 5---\n\n")
# reviews_data.show(n=5, truncate=False) #this should work

first_five_reviews = spark.sql("SELECT * FROM reviews LIMIT 5") #this also works
first_five_reviews.show(truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?

print("\n\n--- Question 6---\n\n")
# print(reviews_data.schema)
# value_of_product = spark.sql("SELECT product_category, COUNT(*) FROM reviews GROUP BY product_category")

# TODO Ask how to do this with SQL?

product_categories = reviews_data.select("product_category")
product_categories.show(50)

# Count occurrences of each product category and order by count descending
most_common_category = (
    product_categories.groupBy("product_category")
    .count()
    .orderBy("count", ascending=False)
)

# Show the most common product category
most_common_category.show(1)

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?

print("\n\n--- Question 7---\n\n")

most_helpful_review = reviews_data.sort(reviews_data.helpful_votes.desc())
most_helpful_review.show(1)

# Question 8: How many reviews exist in the dataframe with a 5 star rating?

print("\n\n--- Question 8---\n\n")

five_star_reviews = (
    reviews_data.groupBy("star_rating")
    .count()
    .orderBy("count", ascending=False)
)
five_star_reviews.show(1)

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
#those are star_rating, helpful_votes, and total_votes
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.

print("\n\n--- Question 9---\n\n")

# df.select(df.name, (df.age + 10).alias('age')).show() - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select
# https://sparkbyexamples.com/pyspark/pyspark-cast-column-type/

cast_to_int = reviews_data.select(
    reviews_data.star_rating.cast('int').alias('star_rating'),
    reviews_data.helpful_votes.cast('int').alias('helpful_votes'),
    reviews_data.total_votes.cast('int').alias('total_votes')
)
cast_to_int.show(10)

# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.

print("\n\n--- Question 10---\n\n")

date_of_purchase = (
    reviews_data.groupBy("purchase_date")
    .count()
    .orderBy("count", ascending=False)
)
date_of_purchase.show(1)

##TODO Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.json.html#pyspark.sql.DataFrameWriter.json

print("\n\n--- Question 11---\n\n")
dest_folder_path: str = os.path.abspath("week2_sql/question11_output")
reviews_data_with_timestamp.write.json(dest_folder_path, mode="overwrite")

print("We got past the write function, check and see if it worked")

### Teardown
# Stop the SparkSession
print("\n \n --- End of Program --- \n \n")
spark.stop()