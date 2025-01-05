# 🚗 Vehicle Sales Data

## 🌟 Overview

The **Vehicle Sales Data** is designed to process and analyze data related to vehicle sales from multiple sources, such as CSV files, APIs, and SQL databases. The project uses **Azure Data Factory (ADF)** for data ingestion and transformation, **Databricks** for advanced processing and cataloging, and **SQL Warehouse** for querying and analytics. The **Medalion Architecture** is implemented in this project.

The data is organized into three layers:
- **Bronze**: Raw data stored in Parquet format.
- **Silver**: Transformed and cleansed data in Parquet format.
- **Gold**: Aggregated data ready for analytics, stored as facts and dimensions in Native(Delta) format.

## 🛠️ Workflow

1. **📥 Data Ingestion**:
   - Data is extracted from GitHub (CSV files), APIs, and SQL databases into SQL DB using Azure Data Factory pipelines.

2. **🔄 Incremental Loading**:
   - High Water Mark logic is implemented to load only new or updated records into the Bronze layer in ADLS.

3. **📂 Data Transformation**:
   - Databricks is used to create a catalog and schemas for  Silver and Gold layers.
   - Data transformations are applied, and the results are stored in ADLS as well.

4. **📊 Data Aggregation**:
   - Facts and dimensions are created for the Gold layer, enabling advanced analytics.

5. **🔒 Credential Management**:
   - Secrets for accessing various services are stored securely in Azure Key Vault and accessed via app registrations.

6. **📈 Analytics**:
   - SQL Warehouse is used to run aggregation queries and generate insights.

## 📂 Repository Structure
```plain.text
Vehicle-Sales-Data-Project/
├── databricks/             # Databricks notebooks for transformations
├── dataset/                # Datasets in ADF
├── datasets/               # Sample data used for testing
├── factory/                # ARM templates and Global Parameters
├── linkedservice/          # Linked services in ADF
├── pipeline/               # ADF pipelines configurations
├── architecture/           # Architecture diagram
├── README.md               # Project overview (this file)
```
## 🚀 Key Features

- 📤 **Data Extraction**: Automated data ingestion from multiple sources.
- 🧹 **Data Cleansing**: Transform raw data into structured formats.
- 📜 **Cataloging**: Organized data into Silver and Gold schemas.
- 🔁 **Incremental Loading**: Efficiently loads new/updated records using High Water Mark logic.
- 🔒 **Secure Credentials**: Managed securely via Azure Key Vault.
- 📊 **Analytics Ready**: Fact and dimension tables for data aggregation and analysis.

## 🛠️ Technologies Used
- 🗄️ **Azure Data Lake Storage (ADLS)**: Centralized storage for raw and processed data.
- 🔄 **Azure Data Factory (ADF)**: Orchestration tool for data pipelines.
- 🔐 **Azure Key Vault**: Securely manages access credentials for services.
- 🧪 **Azure Databricks**: Handles data transformations and cataloging.
- 📜 **SQL Warehouse**: Enables querying and advanced analytics.

## 🚀 How to Use
1. **🔑 Setup Credentials**:
   - Store necessary credentials in Azure Key Vault and grant access to app registrations.
2. **🔄 Run Pipelines**:
   - Use ADF pipelines to ingest data into the Bronze layer.
3. **📂 Process Data**:
   - Execute Databricks notebooks to process data into Silver and Gold layers.
4. **📊 Analyze Data**:
   - Use SQL Warehouse to run aggregation queries and generate insights.

## 🌟 Future Enhancements
- 🧠 Add ML models for predictive sales analytics.
- 📊 Create real-time dashboards for sales insights.
- ⚡ Optimize pipeline performance with partitioning and caching.

## 📬 Contact
For any questions or support, reach out to the repository maintainer at **[vignanpatchigolla5@gmail.com]**.
