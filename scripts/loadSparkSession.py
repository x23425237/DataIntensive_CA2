import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, current_timestamp


#%%
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()



