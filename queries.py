from SS import Spark as SS 

df = SS.read.csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv", header='true')

df.createOrReplaceTempView("Test1") 

SS.sql("SELECT * FROM Test1").show()

SS.sql("SELECT `Draw Date` FROM Test1").show()

SS.sql("SELECT * FROM Test1 WHERE RIGHT(`Draw Date`, 4) < 2018").show()
