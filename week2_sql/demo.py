from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Demo") \
    .master("local") \
    .getOrCreate()

version = spark.version
print(f"If I've started a cluster running spark, this is the version {version}")

scores_data = spark.read.csv("resources/video_game_scores.tsv", sep=",", header=True)
scores_data.printSchema()
scores_data.show(n=4, truncate=False)

scores_data.write.json("resources/video_game_scores_json")

print("End of Program")
spark.stop()