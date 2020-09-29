"""
Created on Tues Sep 29 2020 21:00 IST

Problem statement: To find the most popular movie IDs using dataframes

Hint: Movie that is most rated is the most popular movie. (Number of times movie is rated.)

Data set: movie lense data downloaded from https://grouplens.org/datasets/movielens/

Data header:    userId movieId rating timestamp
                196	242	3	881250949
                186	302	3	891717742
                22	377	1	878887116
                244	51	2	880606923
"""
__author__      = "Sayali Joshi"

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create explicit schema as data file does not have header
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])

# Load movie data into a dataframe
movieDF = spark.read.option("sep", "\t").schema(schema).csv("u.data")

popMovies = movieDF.groupBy("movieId").count().orderBy(func.desc("count"))

# Show top 10 popular movies
popMovies.show(10)

spark.stop()
