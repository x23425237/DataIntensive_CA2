import loadSparkSession as load
import sys 
from pyspark.sql.functions import col,date_format
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



# load the race file 
df_race=load.spark.read\
.option("header",True)\
.csv('./data/races.csv')

# view the data
df_race.show()
#print schema
df_race.printSchema()

# calculate the the size of the dataframe
rows = df_race.collect()
df_size_bytes = sys.getsizeof(rows)
print(f"Estimated size of the DataFrame = {df_size_bytes} MB")


# select required columns by index(excluding url)
race_finial=df_race.select(df_race.columns[:7])
race_finial.show()

#add timeframe to dataframe

write_data=race_finial.withColumn("timestamp", load.lit(load.current_timestamp())) 
write_data.show()

# change the timestamp format 
df=write_data.select(
    col("raceID"),
    col("year"),
    col("round"),
    col("circuitID"),
    col("name"),
    col("date"),
    col("time"),
    date_format(col("timestamp"), "MM-dd-yyyy HH:mm:ss").alias("date_time_format")
)


#write to output folder
outputfolder="output/race_out.csv"
df.write.csv(outputfolder,header=True,mode="overwrite")

