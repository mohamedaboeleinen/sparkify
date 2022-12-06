# Data Modeling with Postgres Project

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.[^1]

[^1]: copied form the project description given in the udacity course.

## Prerequisites 

You will need Python 3, pandas, psycopg2 and a PosgreSQL db running in your localhost.

## Project Structure
### data
A directory containing the raw data for the etl pipeline. It contains two subdirectories: log_data and song_data. Each subdirectory contains the .json files with the raw data records. 
### create_tables.py
A python script used to reset the database (drop the tables if they exist) and create the tables. You need to run this script before you run the etl script, or the notebooks. 
### etl.ipynb
A jupyter notebook containing the etl steps to process the data files and push them into the database. You need to run create_tables.py before you can run this. You need to close the connection to the database or restart the notebook before you can run the etl.py.
### README.md
### sql_queries.py
contains the sql queries to drop and create the database tables and also the insert statements. It is used by the etl.py, etl.ipynb and test.ipynb
### test.ipynb
This is used to verify that the data has been inserted to the database and for sanity checks before submitting the project. 
## How to run the python scripts? 
´´´python create_tables.py ´´´
´´´ python etl.py ´´´
