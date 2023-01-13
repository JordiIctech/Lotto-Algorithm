from SS import Spark as SS 

df = SS.read.csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv", header='true')

df.show()
