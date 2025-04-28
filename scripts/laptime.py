# load all driver data 
#load session data 
# select exclusion rules in the column selection
# inner join session data and driver data 
#https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/


import loadSparkSession as load

from pyspark.sql.functions import col

#http://ergast.com/images/ergast_db.png


df4 = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv("C:/Users/prana/DataIntensive_CA2/Kaggle/allDrivers.csv")

# show data 
df4.show()


# load laptime dataset 

df5 = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv("C:/Users/prana/DataIntensive_CA2/Kaggle/lap_times.csv")

races = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv("C:/Users/prana/DataIntensive_CA2/Kaggle/races.csv")

# show data 
df5.show()

# print schema 
df5.printSchema()
df4.printSchema()
races.printSchema()

# Full Outer Join
# join drivers and laptime by driver id 

# create temp view 
df5.createOrReplaceTempView("df5") #/ 589081
df4.createOrReplaceTempView("df4") #4799
races.createOrReplaceTempView("races") #1125

load.spark.sql("""
 select * from races
        
""").show()


# how many races per year

load.spark.sql("""
 select count(raceID) as count ,year from races
               group by year
               order by count desc
        
""").show()




# how many circuits per year

load.spark.sql("""
 select count(circuitid),year from races
group by year
order by year desc     
""").show()




df_lap = load.spark.sql("""
    SELECT 
        df5.raceId,
        races.year,
        races.circuitId,
        races.name,
        df5.driverId,
        df5.lap,
        df5.position,
        df5.milliseconds,
        df4.first_name,
        df4.last_name,
        df4.full_name,
        df4.meeting_key,
        df4.session_key,
        df4.team_colour,
        df4.team_name
    FROM df5 
    INNER JOIN df4 ON df4.driver_number = df5.driverId
    INNER JOIN races ON df5.raceId = races.raceId
""")


df_lap.show(truncate=False)


df_lap.createOrReplaceTempView("df_lap_view")



load.spark.sql("""
 select count(distinct(driverID)) from df_lap_view
        
""").show()



#total number of rows per year
load.spark.sql("""
    select count(*),year from df_lap_view
   group by year
   order by year desc
        
""").show()


# count number of drivers in each race. 

load.spark.sql("""
     select count(distinct(driverid)),year from df_lap_view
   group by year
   order by year desc
        
""").show()

