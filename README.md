# Main concepts
This is a Data Engineering project containing both web automation processes and an ETL workflow. Also a few dashboards to top it out.
As a person highly interested and connected with transport and travel, i started searching for this topic related project opportunities and found an interesting open data platform:  
[**Open-Data Platform Mobility Switzerland**](https://opentransportdata.swiss/en/)

[PDF Project Overview](SWISS_TRANSPORT-Overview.pdf)

This project consists of **fully automated ETL workflow** with following stages:
- Scheduled download and processing of total **12 data files** with various formats (.csv, .xlsx, .json) - using **Selenium**
- File downloads are divided into 4 separate frequencies: daily, weekly, monthly, yearly - depending on data type and data source update frequency
- Automated file ingestion to **Snowflake** via **SnowSQL**, right after file is successfully processed
- Transformations & Final tables output using **Snowpark**
- All that scheduled in **Airflow** (5 different DAGs), running on a **Docker** container.

Consumption-ready data is analyzed in **Qlik Sense** sheets (using Snowflake connection) - with 3 separate Dashboards as an output.

# How to run this project
## Step 1. - Airflow Installation & Setup  
  For this step make sure You have Docker Installed on Your computer.  
As the Airflow installation procedure is pretty much well known already, the standard steps apply. All Docker configuration files are in the project main directory, so it's enough to create an admin user,
the image:  
```
docker build -t airflow_image .
```
and also the compose command  
```
docker-compose up -d
```

Also remember to create snowflake connection in the airflow web UI using Snowflake account credentials & database info.  

## Step 2. - Snowflake Database Creation  
  All Snowflake SQL scripts are ready to run and create database with all the schemas  

## Step 3. - .env File Creation  
  The .env file should be present in project root directory and contain following variables defined:  
SNOWFLAKE_ACCOUNT=  
SNOWFLAKE_USER=  
SNOWFLAKE_PASSWORD=  
SNOWFLAKE_ROLE=SYSADMIN  
SNOWFLAKE_DATABASE=SWISS_TRANSPORT  
SNOWFLAKE_SCHEMA=RAW  
SNOWFLAKE_WAREHOUSE=TRANSPORT_WH  

## Step . Running the DAGs
  The files are downloaded with various frequencies which are free to change. Adding/removing files from a frequency(thus DAG workflow) is possible only by changes in config.json file.  
Main 4 DAGs are generated dynamically in one script, while the 5th, externally triggered one is configured in separate file. All DAGs could be in separate files as well, but that would create so much code repetition.  


# Additional Project Info  
  - Some files take longer to download and/or process, depending on data category and file type - some files need rows removal etc.
  - Download frequencies (thus file separation) are intuitive and not necessarily critical - all files could be downloaded and processed daily, but what's the point if some files are updated on the page monthly or even yearly
  - This project can be run and tested without Airflow and Docker using local_script_files on Your local machine
  - Project is meant to be fully automated from start to finish
