"""
Created on Tues Sep 30 2020 18:38 IST

Problem statement: To find the most popular marvel star who has highest costar connections.

Hint: The marvel_Graph.txt i.e. space separated has first field as marvel start id and all rest fields are his
co-appearances, call it connections. We have to find which marvel star id has highest number of connections.
In order to show the result, we have to show the marvel star name instead of id. Here we have to make use of
mapping file marvel+names.txt.

"""
__author__ = "Sayali Joshi"


from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("PopularMarvelStar").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])

names = spark.read.option("sep", " ").schema(schema).csv("datasets/Marvel+Names.txt")

lines = spark.read.text("datasets/Marvel+Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))

#connections.show(2)

mostPopularConnections = connections.sort(func.col("connections").desc()).first()
# print(mostPopularConnections)

# mostPopularName df has only one column name
mostPopularName = names.filter(func.col("id") == mostPopularConnections[0]).select("name").first()

print(mostPopularName[0] + " is the most popular marvel star with highest connections with co-appearances as "
      + str(mostPopularConnections[1]))

spark.stop()
