from SS import Spark as SS

df = SS.read.option("delimiter", ",").option("header", "true").csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")

df.createOrReplaceTempView("Test1") 
