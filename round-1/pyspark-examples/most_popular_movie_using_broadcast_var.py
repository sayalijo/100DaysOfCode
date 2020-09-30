"""

Created on Tues Sep 29 2020 21:00 IST

Problem statement: To find the most popular movie IDs using dataframes but display movie name from u.items (not movieIDs)

Hint: Movie that has most rating is the most popular movie. To show movie name, we have to use two datasets. We want to avoid extra overhead of shuffling data
to join 2 dataframes. So another efficient way is to use broadcast variable.

Data set: u.data - movie lense data downloaded from https://grouplens.org/datasets/movielens/
Data header:    userId movieId rating timestamp
                196	242	3	881250949
                186	302	3	891717742
                22	377	1	878887116
                244	51	2	880606923

Mapping dataset: u.item - mapping of movieId and movieNames
Data header:    movieId|movieName|release_date|......
                1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
                2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0


"""
__author__ = "Sayali Joshi"

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open("datasets/u.item", mode="r", encoding="iso-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split(sep="|")
            movieNames[int(fields[0])]=fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMoviesBroadcastTechnique").getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create explicit schema as data file does not have header
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])

movieDF = spark.read.option("sep", "\t").schema(schema).csv("datasets/u.data")
movieCounts = movieDF.groupBy("movieId").count()


def movieLookup(movieId):
    return nameDict.value[movieId]


lookupUDF = func.udf(movieLookup)

# We want to append a column to movieCounts dataframe called as "movieTitle" whos content is lookupUDF().
# We have to first convert python def into an UDF func so that we can it within sparkSQL
moviesWithNames = movieCounts.withColumn("movieTitle", lookupUDF(func.col("movieId")))

# Sort the movies according to most rated count
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()


