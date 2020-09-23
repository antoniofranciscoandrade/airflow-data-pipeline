# Data Warehouse automation using Airflow

This project aims to automate the data warehouse and ETL pipelines for song data using Apache Airflow. The goal is to build data pipelines that are dynamic and built from reusable tasks, and that can also be monitored and allow easy backfills. Data quality is also improved by running tests against the dataset after the ETL steps are complete in order to catch any discrepancies.

The source data is in S3 and needs to be processed and loaded into Amazon Redshift. The source data are CSV logs containing user activity in the application and JSON metadata about the songs the users listen to.

## Airflow Tasks

Custom operators are created to perform tasks such as staging the data, filling the data warehouse and running checks. The tasks are linked together to achieve a coherent and sensible data flow within the pipeline. 

## Project Template

- There are three major components of the project:
1. Dag template with all imports and task templates.
2. Operators folder with operator templates.
3. Helper class with SQL transformations.
- Add `default parameters` to the Dag template as follows:
    * Dag does not have dependencies on past runs
    * On failure, tasks are retried 3 times
    * Retries happen every 5 minutes
    * Catchup is turned off
    * Do not email on retry
- The task dependencies should generate the following DAG:

![dag](dag.png)

- There are four operators:
1. Stage operator 
    * Loads JSON and CSV files from S3 to Amazon Redshift
    * Creates and runs a `SQL COPY` statement based on the parameters provided
    * Parameters should specify where in S3 file resides and the target table
    * Parameters should distinguish between JSON and CSV files
    * Contain a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills
2. Fact and Dimension Operators
    * Use SQL helper class to run data transformations
    * Take as input a SQL statement and target database to run query against
    * Define a target table that will contain results of the transformation
    * Dimension loads are often done with truncate-insert pattern where target table is emptied before the load
    * Fact tables are usually so massive that they should only allow append type functionality
3. Data Quality Operator
    * Run checks on the data
    * Receives one or more SQL based test cases along with the expected results and executes the tests
    * Test result and expected results are checked and if there is no match, operator should raise an exception and the task should retry and fail eventually

## Build Instructions

Run `/opt/airflow.start.sh` to start the Airflow server. Access the Airflow UI by clicking `Access Airflow` button. Note that Airflow can take up to 10 minutes to create the connection due to the size of the files in S3. 