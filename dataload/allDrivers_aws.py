
# %%
import loadSparkSession as load
import requests
import json
import os


# %%
# STEP 1 : fectch drivers data from drivers api and load to csv 
#https://openf1.org/?python#meetings

# function to fetch race data from api 
def fetch_race_data(url,dataframe:str):
    response = requests.get(url)
    df=response.json()
    df = load.spark.read.json(load.spark.sparkContext.parallelize([json.dumps(df)]))
    df.show()
    df.printSchema()

    #write to output folder
    outputfolder=f"output/{dataframe}.csv"
    df.write.csv(outputfolder,header=True,mode="overwrite")


""""
latest car data for max v 

"""
url_2025 = "https://api.openf1.org/v1/car_data?driver_number=81&session_key=10022&speed%3E0"
folder="cardata_81"
fetch_race_data(url_2025,folder)





# STEP 2 : Connect to S3 bucket and load driver's data to s3

# %%
import boto3
import pandas as pd 
import os


s3=boto3.resource (
    service_name='s3',
    region_name='',
    aws_access_key_id='',
    aws_secret_access_key=''
)


for bucket in s3.buckets.all():
    print(bucket.name)


os.environ["AWS_DEFAULT_REGION"]='eu-north-1'
os.environ["AWS_ACCESS_KEY_ID"]=''
os.environ["AWS_SECRET_ACCESS_KEY"]=''


# upload files to s3 bucket
filename='C:/Users/prana/DataIntensive_CA2/dataload/output/cardata_81.csv/cardata_oscar.csv'
s3.Bucket('formulaonebr').upload_file(Filename=filename,Key='cardata_oscar.csv')




# print all ojects in s3 
for obj in s3.Bucket('formulaonebr').objects.all():
    print(obj)




# %%
s3.Bucket('formulaonebr').Object('cardata_oscar.csv').get()

# %%
# load csv file as dataframe 
obj=s3.Bucket('formulaonebr').Object('cardata_oscar.csv').get()
foo=pd.read_csv(obj['Body'],index_col=0)



# %%
foo.head()


# Setp 5
# perform data pre-processing - change date and time column to milli seconds 
# load data back into the docker volumes

df4 = load.spark.read\
.options(header=True)\
.options(inferSchema="true")\
.csv(filename)

# show data 
df4.show()


#%%
# change date column to milliseconds 
from pyspark.sql.functions import unix_timestamp, col, to_timestamp


df_millis = df4.withColumn(
    "millis",
    (col("date").cast("timestamp").cast("double") * 1000).cast("long")
)

df_millis.show()

#%%
# drop date column

#df_millis = df_millis.drop("date")

# same file to kaggle folder
output_path='C:/Users/prana/DataIntensive_CA2/Kaggle/cardata_oscar.csv'
df_millis.coalesce(1).write.option("header", "true").csv(output_path)


# STEP 6 : Data orchestration using airflow - connect s3 and airflow
# load drivers datainto postgres 








# %%
