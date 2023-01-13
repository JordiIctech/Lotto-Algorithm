from SS import Spark as SS #Imports Spark Session from separate file
import time
#====================================================================================================
start_time = time.time()
#----------------------------------------------------------------------------------------------------
# df = SS.read.csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv", header='true')
df = SS.read.option("delimiter", ",").option("header", "true").csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")
#Only taking second delimiter option

df.show()

#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================