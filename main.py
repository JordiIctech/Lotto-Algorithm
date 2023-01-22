from SS import Spark as SS #Imports Spark Session from separate file
import time
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
number = 54
dfE = SS.sql(f"""SELECT Count(`Winning Numbers`) FROM Test1 WHERE `Winning Numbers` LIKE '%{number}%'""")

dfE.show()

valueE = dfE.head()[0]

print(valueE)

for i in range(5):
    print(i+1)

#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================