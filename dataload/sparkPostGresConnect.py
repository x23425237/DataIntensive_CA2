
import loadSparkSession as load
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import sql
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType



# Define PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5432/FormulaOne"  # Replace with your PostgreSQL details
properties = {
    "user": "postgres",  # Replace with your username
    "password": "dap",  # Replace with your password
    "driver": "org.postgresql.Driver"
}

# read data drivers.csv
# read results.csv

driverSchema = StructType([
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True)
])

# load the race file 
df_driver=load.spark.read\
.option("header",True)\
.schema(driverSchema)\
.csv('./Kaggle/drivers.csv')

df_driver.show()
df2_driver=df_driver.drop("url")
df2_driver.show()

# define the schema 


df2_driver.printSchema()

# SQL query to create the table
create_table_sql = """
CREATE TABLE IF NOT EXISTS formula1.drivers (
    driverId INT,
    driverRef VARCHAR(255),
    number INT,
    code VARCHAR(10),
    forename VARCHAR(255),
    surname VARCHAR(255),
    dob DATE,
    nationality VARCHAR(100)
);
"""
db_host = 'localhost'  # Host where PostgreSQL is running
db_name = 'FormulaOne'  # Name of the database
db_user = 'postgres'    # Your PostgreSQL username
db_password = 'dap'     # Your PostgreSQL password

conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password
)
 
cursor = conn.cursor()
cursor.execute(create_table_sql)
conn.commit()

table_name="formula1.drivers"

## load data to postgres
df2_driver.write.jdbc(url=url, table=table_name, mode="append", properties=properties)











