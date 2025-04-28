
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
allSession

"""
url_2025 = "https://api.openf1.org/v1/sessions"
folder="allSession"
fetch_race_data(url_2025,folder)



