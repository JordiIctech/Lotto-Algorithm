import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

Spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = Spark.read.csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")

df.show()