
import loadSparkSession as load

import requests
import json
import os



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
2024/
9481
9482
9483
9484
9488
"""
url_2025 = "https://api.openf1.org/v1/drivers?driver_number%3C=100&session_key=9488"
folder="2024_driver_9488"
fetch_race_data(url_2025,folder)


