# Redfin-Analytics-DE-Pipeline-Airflow-AWS-Snowflake-PowerBI

# Overview
This project is a full-stack data pipeline that extracts real estate data from Redfin, stores and processes it using Amazon S3, EC2, Apache Airflow, and Snowflake, and finally visualizes insights using Power BI.

# Project Goals
1. **Data Ingestion** : Ingest real estate listing data from the Redfin API using a Python script hosted on Amazon EC2. Utilize Apache Airflow to orchestrate automated ETL pipelines that extract data and store it in Amazon S3 in its raw format.
2.  **ETL System** : Use Airflow to trigger Python-based transformation scripts that clean, enrich, and prepare the Redfin data. The ETL process includes type conversion, null handling etc.
3. **Data Lake with Medallion Architecture**: Implement a architechture to store the raw and transformed data in S3 buckets.
4. **Cloud-Native Architecture** : The project is fully hosted on AWS Cloud, using: **EC2 for script execution and orchestration with Airflow,  S3 for layered data storage
Snowflake as the centralized cloud data warehouse, Snowpipe for continuous data ingestion from S3 to Snowflake.**
5. **Reporting and Visualization**: Connect Power BI to the Gold layer in Snowflake to create rich, interactive dashboards. Visualize key real estate insights.

## Services Used

1. **Amazon S3** : Amazon S3 is a scalable object storage service used to store raw, transformed, and analytics-ready data. 
2. **Amazon EC2**: EC2 provides resizable compute capacity in the cloud. It is used to host and run custom Python scripts and the Apache Airflow instance responsible for orchestrating the ETL pipeline.
3. **Apache Airflow**: Apache Airflow is an open-source workflow orchestration tool. 
4. **Snowflake**: Snowflake is a cloud-native data warehouse used to store and analyze large volumes of transformed real estate data. 
5. **Snowpipe**: Snowpipe is a continuous data ingestion service in Snowflake. 
6. **Microsoft Power BI**: Power BI is a business intelligence and data visualization tool.

## Dataset Used
1. Redfin provides month-wise real estate data including metrics like median sale price, homes sold, and days on market.
2. The data is segmented by City and State, allowing for comparative trend analysis across different markets.
3. Data Source Citation : https://www.redfin.com/news/data-center/

# High Level Architechture Diagram
![redfindag](https://github.com/user-attachments/assets/523c4c4f-7ffa-4632-b699-c42d30d509ab)
*Figure 1 : High Level Architechture Diagram of the Pipeline*

# Process Flow
1. **Data Extraction from Redfin API → EC2 → S3** : A Python script running on an Amazon EC2 instance is scheduled via Apache Airflow to extract real estate data from the Redfin API. The fetched data ( CSV format) is saved as-is to an S3 bucket in the raw/ directory.
2. **Data Transformation with Apache Airflow (EC2)** : Airflow triggers a transformation script that Cleans the raw data handles (missing values, standardizes formats, renames columns). The transformed data is saved back into S3 under the transformed/ directory.

3. **Loading Transformed Data to Snowflake using Snowpipe**:  Snowpipe is configured to continuously monitor the transformed/ folder in the S3 bucket. As new files arrive, they are automatically loaded into a staging table in Snowflake for further analysis.
4. **Reporting with Power BI**: Power BI is connected directly to the Snowflake warehouse. Business users can build interactive dashboards to visualize key Insights.
<img width="821" height="375" alt="image" src="https://github.com/user-attachments/assets/478a5a58-dafa-42cb-9f05-e9bc7917cff7" />

*Figure 2 : Sample Visualization Dashboard Via PowerBI*


