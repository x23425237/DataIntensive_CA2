# load all driver data 
#load session data 
# select exclusion rules in the column selection
# inner join session data and driver data 
#https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/


import loadSparkSession as load
import pyspark.sql.functions as F
from pyspark.sql.functions import col

#http://ergast.com/images/ergast_db.png


df4 = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv("C:/Users/prana/DataIntensive_CA2/output/allDrivers.csv")

# show data 
df4.show()



# select all columns except headshot_url,first_name,last_name

select_df=df4.select([c for c in df4.columns if c not in {'headshot_url','first_name','last_name'}])

select_df.show()

# load session data 

df5 = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv("C:/Users/prana/DataIntensive_CA2/output/allSession.csv/allSessions.csv")

# show data 
df5.show()

select_session=df5.select([c for c in df5.columns if c not in {'circuit_key','country_key','country_code'}])
select_session.show()# table 1
select_df.show() # table 2 

# PySpark Inner Join DataFrame

# create view 
select_session.createOrReplaceTempView("sessionData")
select_df.createOrReplaceTempView("driverData")

# inner join 
joinDF2 = load.spark.sql("select * from sessionData e "
"INNER JOIN driverData d ON e.session_key == d.session_key")
  
joinDF2.show(truncate=False)

# print columns
print(joinDF2.columns)

# print schema 
joinDF2.printSchema()


# Lewis HAMILTON

leiwis_Hamiltion=joinDF2.where(col("full_name")=="Lewis HAMILTON")

leiwis_Hamiltion.show()

leiwis_Hamiltion.printSchema()

# session_type,year,country_code,country_name,session_type,location,meeting_key,session_key,circuit_short_name

leiwis_Hamiltion.createOrReplaceTempView("Hamiltion")
leiwis_Hamiltion_set1 = load.spark.sql("""
    SELECT 
        session_type, 
        year, 
        country_code, 
        country_name, 
        location, 
        driver_number,
        broadcast_name,
        team_colour,
        circuit_short_name,
        date_start,
        date_end
    FROM Hamiltion
""")

leiwis_Hamiltion_set1.show()

