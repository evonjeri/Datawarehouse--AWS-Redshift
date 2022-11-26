## Datawarehouse - Amazon Redshift
### Introduction
A music streaming startup, wants to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights into what songs their users are listening to.

To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

### Project Datasets
Working with two datasets that reside in S3.

Song data: s3://udacity-dend/song_data Log data: s3://udacity-dend/log_data Log data json path: s3://udacity-dend/log_json_path.json

###  Database Schema for Song Play Analysis

Fact Table 
> songplays 

Dimension Tables
 > users - users in the app
 
 >songs - songs in music database

> artists - artists in music database

> time - timestamps of records in songplays broken down into specific units.

### Project files:
- create_table.py - create fact and dimension tables for the star schema in Redshift.
- etl.py - load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py - define you SQL statements, which will be imported into the two other files above.

### Steps
#### Create Table Schemas Design schemas for your fact and dimension tables
1. Write a SQL CREATE statement for each of these tables in sql_queries.py<br>
2. Complete the logic in create_tables.py to connect to the database and create these tables<br>
3. Launch a redshift cluster and create an IAM role that has read access to S3.<br>
4. Add redshift database and IAM role info to dwh.cfg.<br>
5. Test by running create_tables.py and checking the table schemas in your redshift database.

#### ETL Pipeline
Implement the logic in etl.py to load data from S3 to staging tables on Redshift.<br>
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.<br>
Delete your redshift cluster when finished.<br>