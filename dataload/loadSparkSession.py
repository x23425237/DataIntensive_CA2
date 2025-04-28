import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *

#%%
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()



