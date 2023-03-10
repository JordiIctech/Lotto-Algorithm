from SS import Spark as SS #Imports Spark Session from separate file
import time
import numpy as np
import matplotlib.pyplot as plt

#====================================================================================================
start_time = time.time()
#----------------------------------------------------------------------------------------------------
# df = SS.read.csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv", header='true')
df = SS.read.option("delimiter", ",").option("header", "true").csv("Data/Lottery_Mega_Millions_Winning_Numbers__Beginning_2002.csv")
#Only taking second delimiter option

df.show()

df.createOrReplaceTempView("Test1") 

SS.sql("SELECT * FROM Test1").show()

#df.filter(col("name").like("%rose%")).show()

# Check Mega Ball Numbers
SS.sql("""SELECT * FROM Test1 WHERE `Mega Ball` LIKE '4' OR `Mega Ball` LIKE '04'""").show()

# Check Winning Number appearances
SS.sql("""SELECT * FROM Test1 WHERE `Winning Numbers` LIKE '%22%'""").show()

# Count Winning Numbers
winnerdic = {}

for i in range(0,56):
    
    print(f"Number: {i+1}")

    if i >= 9:
        dfE = SS.sql(f"""SELECT Count(`Winning Numbers`) FROM Test1 WHERE `Winning Numbers` LIKE '%{i+1}%'
        AND `Multiplier` IS NOT NULL""")

    else:
        dfE = SS.sql(f"""SELECT Count(`Winning Numbers`) FROM Test1 WHERE `Winning Numbers` LIKE '%0{i+1}%'
        AND `Multiplier` IS NOT NULL""")
    
    valueE = dfE.head()[0]
    print(f"Count: {valueE}")

    winnerdic[f"{i}"] = {valueE}
print(winnerdic)
#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================