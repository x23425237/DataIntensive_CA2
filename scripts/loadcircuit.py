# %% [markdown]

# #1. Start the spark Spark
# #2. load the circuit dataset
# #3. print schema 
# #4. make subset of dataframe
# #5. write the file to output 



#%%
#!pip install findspark

#%%
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.functions import col,date_format
from pyspark.sql.functions import lit, current_timestamp




#%%
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#%%

df=spark.read\
.option("header",True)\
.option("inferSchema",True)\
.csv('./data/circuits.csv')

#%%
df.head()

df.show()



#%%
df.printSchema()



#%%
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])



#%%
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("data/circuits.csv")
# %%
circuits_df.show()


# %%
# drop url column 

dataset1= circuits_df.select(col("circuitId"), col("circuitRef"), 
                              col("name"), col("location"), 
                              col("country"), col("lat"), 
                              col("lng"), col("alt"))

# %%
dataset1.show()

#%%
# add new column for timestamp 
dataset1.withColumn("timestamp", lit(current_timestamp())) \
  .show()

# %%
outputfolder="output/circuit_out.csv"
dataset1.write.csv(outputfolder,header=True,mode="overwrite")


# %%
# calculate the the size of the dataframe
rows = dataset1.collect()
df_size_bytes = sys.getsizeof(rows)
print(f"Estimated size of the DataFrame = {df_size_bytes} MB")

