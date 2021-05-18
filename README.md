# Data Modeling With Postgres

Project repo for the Udacity Data Engineering Program Project 1 - PostgreSQL.
This README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

## Getting Started

> **NOTE** The default connection parameters are in the db_conn.py file.

1. You will need to first connect and build the tables for the Sparkify database.  To do this run the following command:

    > python create_tables.py

2. To load data into the Sparkify database run the following command:

    >python etl.py

## Prerequisites

1. Baseline configured PostgreSQL database.

2. The following Python libraries need to be installed in the environment.
    * os
    * sys
    * glob
    * psycopg2
    * pandas
    * io

## Purpose

The purpose of this database is to conduct ETL operations and store data from user activity and songs on their Sparkify's new music streaming app.  
This data will be used by the Sparkify analytics team to help understand what songs users are listening to and their activities.

## Database Schema

There are 5 tables in the database. This design focuses on the songplays facts table which have the most important information for the analytics team.  The supporting dimension tables of time, users, songs, and artists help to provide context and additional details for the songplays table.

![](Sparkify ER diagram.png?raw=true)

## ETL Pipeline

The extract, transform and load processes in **elt.py** populate the **songs** and **artists** dimention tables from JSON files in 'data/song_data'. The **time** and **users** dimention tables are populated from the JSON files in 'data/log_date'. Song and artist ID  are collected from form a 'SELECT' query and combined with data derived from the log files and are used to populate the **songplays** fact table.

## Example queries

Number of user for each membership level.

'SELECT level, COUNT(level) FROM users GROUP BY level;'

Top 3 users by session.

'SELECT users.last_name, users.first_name, COUNT(songplays.user_id) FROM songplays INNER JOIN users ON songplays.user_id=users.user_id GROUP BY users.last_name, users.first_name ORDER BY count DESC LIMIT 3;'

User activity breakdown in the App, by hour of the day.

' SELECT hour, COUNT(hour) tmp FROM time GROUP BY hour ORDER BY tmp DESC;'
