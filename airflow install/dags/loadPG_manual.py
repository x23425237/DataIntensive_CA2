import pandas as pd
from sqlalchemy import create_engine

# Hardcoding the PostgreSQL connection parameters
user = 'airflow'
password = 'airflow'
host = 'localhost'
port = '5433'
database = 'airflow'



# Create the PostgreSQL engine using hardcoded connection params
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

dd='C:/Users/prana/DataIntensive_CA2/Kaggle/races.csv'
# Read the CSV file into a DataFrame
data = pd.read_csv(dd)

# Insert data into the 'airbnb_python' table in PostgreSQL
# if_exists='append' ensures data is added without overwriting the existing data
data.to_sql('races', engine, schema='formula1',if_exists='replace', index=False)

# Confirmation message
print('Done!')
