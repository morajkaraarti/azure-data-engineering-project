# azure-data-engineering-project
## Introduction
The AdventureWorks Data Engineering Project is a cloud-based data pipeline built on the Microsoft Azure ecosystem. It demonstrates how raw business data can be ingested, transformed, and modeled into analytical datasets using the medallion architecture approach (Bronze, Silver, Gold). This project showcases essential data engineering skills—including data ingestion with Azure Data Factory, transformation using PySpark in Azure Databricks, and modeling in Azure Synapse Analytics—making it ideal for real-world enterprise use cases

## Architecture
![Architecture](architecture.png)

## Technology Used
1. Azure Data Factory – Orchestrates data ingestion from GitHub to Data Lake
2. Azure Databricks (PySpark) – Cleans, transforms, and enriches the data
3. Azure Synapse Analytics – Stores star-schema modeled data for reporting and querying
4. Data Lake Gen2, Delta Lake, and SQL views for scalable and reliable processing
5. Power BI - For reporting and analtyics.
