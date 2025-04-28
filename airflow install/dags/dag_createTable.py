# airflow dag to create 
# ref : https://www.youtube.com/watch?v=aARngixh2dc
# %%
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'bharathi',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
# %%
with DAG(
    dag_id='dag_with_postgres_operatorF1',
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule_interval='0 0 * * *'
) as dag:

    # Task 1: Create the table in PostgreSQL
    task1 = PostgresOperator(
        task_id='create_tableF1_constructor_results',
        postgres_conn_id='postgres_local',
        sql="""
            CREATE TABLE IF NOT EXISTS formula1.constructor_results (
                constructorResultsId SERIAL PRIMARY KEY,
                raceId INTEGER,                 
                constructorId INTEGER,         
                points DOUBLE PRECISION, 
                status VARCHAR(50)
            );
        """
    )

    # Task 2: Load data from CSV into PostgreSQL
    task2 = PostgresOperator(
        task_id='create_tableF1_constructor_standings',
        postgres_conn_id='postgres_local',
        sql="""
            CREATE TABLE formula1.constructor_standings (
                    constructorStandingsId SERIAL PRIMARY KEY,   
                    raceId INTEGER NOT NULL,                     
                    constructorId INTEGER NOT NULL,              
                    points DOUBLE PRECISION,                        
                    position INTEGER,                          
                    positionText Varchar(50),                   
                    wins INTEGER                                
                );

        """
    )

# Task 3: Load data from CSV into PostgreSQL
    task3 = PostgresOperator(
        task_id='create_tableF1_constructor',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.Constructors (
                    constructorId INT PRIMARY KEY,
                    constructorRef VARCHAR(50) NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    nationality VARCHAR(50),
                    url VARCHAR(255)
);

        """
    )

# Task 4: Load data from CSV into PostgreSQL
    task4 = PostgresOperator(
        task_id='create_tableF1_driver_standings',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.DriverStandings (
                        driverStandingsId INT PRIMARY KEY,
                        raceId INT,
                        driverId INT,
                        points INT,
                        position INT,
                        positionText VARCHAR(50),
                        wins INT );
        """
    )

# Task 5: Load data from CSV into PostgreSQL
    task5 = PostgresOperator(
        task_id='create_tableF1_driverS',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.Drivers (
                    driverId INT PRIMARY KEY,
                    driverRef VARCHAR(50) NOT NULL,
                    number INT,
                    code VARCHAR(10),
                    forename VARCHAR(100) NOT NULL,
                    surname VARCHAR(100) NOT NULL,
                    dob DATE,
                    nationality VARCHAR(50),
                    url VARCHAR(255)
                                    );

        """
    )

# Task 6: Load data from CSV into PostgreSQL
    task6 = PostgresOperator(
        task_id='create_tableF1_laptimes',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.LapTimes (
                        raceId INT,
                        driverId INT,
                        lap INT,
                        position INT,
                        time TIME,
                        milliseconds INT
    
);
        """
    )

# task 7 
    task7 = PostgresOperator(
        task_id='create_tableF1_pitstops',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.pitstops (
                        raceId INT,
                        driverId INT,
                        stop INT,
                        lap INT,
                        time TIME,
                        duration TIME,
                        milliseconds INT
                        
);
        """
    )
# task 8
    task8 = PostgresOperator(
        task_id='create_tableF1_qualifying',
        postgres_conn_id='postgres_local',
        sql="""
                CREATE TABLE formula1.qualifying(
                        qualifyId INT PRIMARY KEY,
                            raceId INT,
                            driverId INT,
                            constructorId INT,
                            number INT,
                            position INT,
                            q1 TIME,
                            q2 TIME,
                            q3 TIME             
);
        """
    )


# task 9

    task9 = PostgresOperator(
        task_id='create_tableF1_races',
        postgres_conn_id='postgres_local',
        sql="""
        CREATE TABLE formula1.Races (
            raceId INT PRIMARY KEY,
            year INT,
            round INT,
            circuitId INT,
            name VARCHAR(100),
            date DATE,
            time TIME,
            url VARCHAR(255),
            fp1_date DATE,
            fp1_time TIME,
            fp2_date DATE,
            fp2_time TIME,
            fp3_date DATE,
            fp3_time TIME,
            quali_date DATE,
            quali_time TIME,
            sprint_date DATE,
            sprint_time TIME
);

        """
    )

# task 10 

    task10 = PostgresOperator(
        task_id='create_tableF1_results',
        postgres_conn_id='postgres_local',
        sql="""
        CREATE TABLE formula1.Results (
        resultId INT PRIMARY KEY,
            raceId INT,
            driverId INT,
            constructorId INT,
            number INT,
            grid INT,
            position INT,
            positionText VARCHAR(10),
            positionOrder INT,
            points FLOAT,
            laps INT,
            time VARCHAR(50),
            milliseconds INT,
            fastestLap INT,
            rank INT,
            fastestLapTime VARCHAR(20),
            fastestLapSpeed FLOAT,
            statusId INT
);

        """
    )


task1 >> task2
task3 >> task4
task5 >> task6
task7>> task8
task9 >> task10
