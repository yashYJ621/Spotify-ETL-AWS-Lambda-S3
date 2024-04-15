![System Diagram](https://github.com/yashYJ621/Spotify-ETL-AWS-Lambda-S3/raw/main/spotify%20etl.png)
# Spotify-ETL-AWS-Lambda-S3
This project implements a data pipeline on AWS for analyzing Spotify playlist data. The ETL process involves extracting data using Python and Spotipy, transforming it into CSV format, and loading it into Snowflake for analysis.

ETL Process Overview
Data Extraction:

Used Python and Spotipy library to extract playlist data from Spotify.
Scheduled data extraction using AWS EventBridge.
Data Transformation:

Transformed raw JSON data into CSV format for analysis.
Data Loading:

Established a connection between AWS S3 and Snowflake for data transfer.
Loaded transformed data into Snowflake for analysis.
