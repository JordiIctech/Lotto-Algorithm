from SS import Spark as SS

df = SS.read.option("delimiter", ",").option("header", "true").csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")

df.createOrReplaceTempView("Test1") 

i = 13

dfE = SS.sql(f"""SELECT * FROM Test1 WHERE `Winning Numbers` LIKE '%{i}%'
    AND `Multiplier` IS NOT NULL""").show()

#-------------------------------------------------------------------------------
from pyspark.sql.functions import split, year, month, dayofmonth, concat_ws, to_date

# Create a sample DataFrame
df = spark.createDataFrame([("01/01/2022",),("01/02/2021",)], ["date"])

# Split the date column by "/"
df_split = df.withColumn("date_split", split(df["date"], "/"))

# Extract the year, month, and day from the split date column
df_result = df_split.withColumn("year", year(df_split["date_split"][2]))
df_result = df_result.withColumn("month", month(df_split["date_split"][0]))
df_result = df_result.withColumn("day", dayofmonth(df_split["date_split"][1]))

# Concatenate the year, month, and day columns into a new date column
df_result = df_result.withColumn("new_date", to_date(concat_ws("-", df_result["year"], df_result["month"], df_result["day"])))

# Filter the DataFrame to only show values after a specific date
specific_date = "2021-01-01"
df_filtered = df_result.filter(df_result["new_date"] > specific_date)

# Drop the split date column and show the result
df_filtered = df_filtered.drop("date_split").drop("new_date")
df_filtered.show()
