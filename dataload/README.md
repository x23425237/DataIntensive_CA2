# Driver Performance Analysis using Apache Spark and PySpark on Formula 1 Data

Real-time analytics have gained immense popularity in recent times with the evolution of high-speed data networks, machine learning algorithms, real-time scaling, and distributed cloud infrastructure across multiple locations. Instant, on-the-go analytics are now utilized by a wide range of sectors — from team coaches looking to enhance player performance, to team sponsors seeking data-driven decision-making and financial allocation strategies, and even to ensure player safety through continuous monitoring. This paper aims to explore the implementation of near real-time data processing in Formula 1 by leveraging the high-speed, in-memory processing capabilities of Apache Spark, cloud storage via Amazon S3, and data management and visualization through PostgreSQL and Power BI.  To automate and orchestrate the end-to-end pipeline for handling live data, Apache Airflow is employed as the workflow management tool. Formula 1 sports one of the sports where race dynamics change rapidly, data complexity is exceptionally high, with high volume, variety,and velocity of data making it a perfect candidate for big data analytics. 


![image alt](https://github.com/x23425237/DataIntensive_CA2/blob/main/workflow.jpg)



# allDrivers_aws.py,allSessionData.py, raceControl.py, getDataAPI
file is designed to load real-time drivers and seasons data from races held in 2023 - 2025. using Boto3, which is the AWS SDK for Python. It is possible to interact programmatically with AWS S3. Real-time data through open API is stored in Amazon S3 blob storage and shown in the workflow diagram Fig 3.  The data is loaded to local storage from S3 for further analysis. The data extracted from API for year 2023 to 2025 are stored in the local storage unit for further analysis.


# getDatakaggle_api.py 
script is for automating the process of getting the historical data from the Kaggle repository. As the historic data contains several folders. Kaggle has a custom method, such as dataset_download_files, to download the entire folder into the specified local repository. 


# driverAnalysis.py,loadcircuit.py, loadRaceFile.py
contains scripts for data analysis using pyspark. spark.read gives access for spark to read the data frame, inferSchema set to true tells spark to not to treat the columns as string. this tells spark to look at the datatype with in each column. As pySpark supports SQL like structure to query the data and as spark is reading the data frames, Select method from pyspark is used to select necessary columns to create a subset of data.We can create temporary dataframes with in spark session. createOrReplaceTempView is a pyspark data frame method create a temporary view with in spark session. spark lets sql like queries to apply on temporary views. Table join query is performed using spark.sql() . Tables can be joined by different conditions such as inner join, left or right join just like sql. New columns are added where required.Defining structure type while reading the dataframe in spark gives better control over defining 
datatypes while reading data from csv for JSON. It helps faster processing of dataframes which is essential component for big data processing particularly in real time. 

# ml.py, Linear Regression on Driver wins
PySpark is designed to work with large volumes of data. In the context of Formula 1, Machine Learning helps to force outcomes, predict wins, laptimes, and perform time series analysis. Pyspark ML libraries provide the necessary libraries just like Python to operate in a big data environment, which can be trained on distributed data. Table II shows actual wins Vs predicted average wins on performing Linear regression on the Drivers' standings data. Model accuracy measured in Root Mean Square (RMSE) of 0.7113 shows that model predictions differ from actual by 7.1% percent. This can be improved my adding more features such as lap times, weather, and driver standings.

![image alt](https://github.com/x23425237/DataIntensive_CA2/blob/main/linearRegression.jpg)

# Data Orchestration using Airflow
docker-compose.yaml is used to configure Apache Airflow in docker container using PostgreSQL as a database. Data volumes are mounted inside docker so that airflow can access the files with in docker container.


# dag_createTable.py,dag10_uploadMultiple.py,dag11_aws.py,loadPG_manual.py,
Several dags are created to automate data orchestration of real time data. To mimic real time data transfer between AWS S3 into PostgreSQL and transfer of historic data into PostgreSQL. The dags are scheduled to run daily to mimic real time environment.dag11_aws is designed to transfer data retrieved from AWS S3. dag10_uploadMultiple scripts loads several historic race details into PostgreSQL on daily basis


# Data Visualization in Power BI

![image alt](https://github.com/x23425237/DataIntensive_CA2/blob/main/powerBI1.jpg)
![image alt](https://github.com/x23425237/DataIntensive_CA2/blob/main/powerBI2.jpg)
![image alt](https://github.com/x23425237/DataIntensive_CA2/blob/main/rpm.jpg)


⚠️ **Note:** This project is still under development. New features and  improvements are continuously being added.

