import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

Spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

