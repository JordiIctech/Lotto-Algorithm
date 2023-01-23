from SS import Spark as SS

df = SS.read.option("delimiter", ",").option("header", "true").csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")

df.createOrReplaceTempView("Test1") 

i = 13

dfE = SS.sql(f"""SELECT * FROM Test1 WHERE `Winning Numbers` LIKE '%{i}%'
    AND `Multiplier` IS NOT NULL""").show()