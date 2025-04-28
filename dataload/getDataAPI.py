#%%
#!pip install requests


#%%
import loadSparkSession as load

import requests
import json
import os


#2025 data
#2024 data 
#2023 data 
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

url_2024 = "https://api.openf1.org/v1/sessions?date_start%3E=2024-01-01&date_end%3C=2024-12-31"
# read data 2024
#folder="2024_race_data"
#fetch_race_data(url_2024,folder)


url_2025 = "https://api.openf1.org/v1/sessions?date_start%3E=2025-01-01&date_end%3C=2025-12-31"
# read data 2025
folder="2025_race_data"
fetch_race_data(url_2025,folder)



