
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
2024/2023 racecontrol

"""
url_2025 = "https://api.openf1.org/v1/race_control?flag=BLACK%20AND%20WHITE&driver_number%3C=100&date%3E=2023-01-01&date%3C=2023-12-31"
folder="2023_raceControl"
fetch_race_data(url_2025,folder)



