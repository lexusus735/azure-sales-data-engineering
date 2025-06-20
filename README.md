# Azure-sales-data-engineering
This is a comprehensive end-to-end data engineering project built on Microsoft Azure. It demonstrates a scalable data pipeline that ingests product sales data from Google Drive, transforms it using Databricks, and stores the output in a Bronze-Silver-Gold architecture using Azure Data Lake Storage Gen2. The final dataset is ready for visualization in Power BI.

---

## Project Overview

This project solves a real-world business use case: automating ingestion, transformation, and aggregation of product sales data for analytical reporting.

**Key Capabilities**:
- Ingests raw CSV files from Google Drive via Azure Data Factory (ADF)

- Cleans and transforms data using Azure Databricks (PySpark)

- Stores data in a multi-layered data lake (Bronze → Silver → Gold)

- Prepares data for downstream analytics tools like Power BI

---

## Solution Architecture

![MS Stack drawio (1)](https://github.com/user-attachments/assets/a5fe97d3-e546-4fd9-8481-1c5a6311c3ab)

---

## Technology Stack

| Component              | Purpose                                      |
|------------------------|----------------------------------------------|
| **Azure Data Factory** | Orchestrates ingestion from Google Drive to ADLS Gen2 |
| **Azure Data Lake Storage Gen2** | Storage for raw, cleaned, and aggregated datasets |
| **Azure Databricks**   | Performs scalable data transformation and cleansing |
| **Google Drive**       | Source of raw CSV sales data              |
| **Power BI**           | Visualization of insights from Gold layer |

---

## Setup Instructions

### Prerequisites

- An active **Azure subscription**
- Access to **Google Drive** containing sales CSV files
- Installed: Power BI Desktop for reporting

---

### Step 1: Azure Environment Setup

#### 1.1 Create a Resource Group

- Sign in to the [Azure Portal](https://portal.azure.com/).
- Navigate to **Resource Groups** and click **+ Create**.
- Provide a name like `sales-data-rg` and choose a region.
- Click **Review + Create**, then **Create**.

#### 1.2 Provision Required Azure Services

Use the Azure Portal to manually create the following resources:

- **Azure Storage Account with Hierarchical Namespace enabled**  
  - Go to **Storage accounts** → **+ Create**.  
  - Enable **Data Lake Storage Gen2** by turning on *Hierarchical namespace*.  
  - After creating the account, create the following containers under *Containers*:
    - `bronze` to store raw data
    - `silver` to store cleaned data
    - `gold` to store aggregated/final data

- **Azure Data Factory**
  - Go to **Data factories** → **+ Create**.
  - Choose the same resource group, region, and give it a unique name.
  - Once deployed, access the ADF Studio to create your pipelines.

- **Azure Databricks**
  - Go to **Azure Databricks** → **+ Create**.
  - Choose the same resource group and region.
  - After deployment, launch the workspace
  - Create a cluster for transformation.

---

### Step 2: Data Ingestion

- **Create a pipeline in Azure Data Factory**:
  - Source: HTTP connector to Google Drive link
  - Sink: Bronze container in ADLS Gen2

- Schedule pipeline with **24 hours trigger**

---

### Step 3: Data Transformation (Azure Databricks)

#### 3.1 Mount ADLS in Databricks
```python
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<client-id>",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kv-scope", key="client-secret"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://bronze@<storage-account>.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)
```
#### 3.2 Transformation and data cleaning
```python
def crm_cust_transform(df):
    try:
        #surogate keys are unique and non null
        df_uniqID =  df.groupBy("cst_id").count().filter(F.col("count") ==1).select(F.col("cst_id"))
        df = df.join(df_uniqID, on="cst_id",how="inner")
    
        # clean cst_key 
        df = df.filter(
        ~(F.length("cst_key") != 10) | F.col("cst_key").rlike("^[0-9]")
            )
    
        #clean and combine names
        df = df.filter(F.col("cst_firstname").isNotNull() | F.col("cst_lastname").isNotNull())\
                .withColumn("cst_fullname",F.concat_ws(" ",F.trim(F.col("cst_firstname")), F.trim(F.col("cst_lastname")))
                           )
    
        # normalize the marital status
        df = df.withColumn("cst_marital_status",  F.when(F.trim(F.upper(F.col("cst_marital_status"))) == "M", "Married")\
                                                   .when(F.trim(F.upper(F.col("cst_marital_status"))) == "S", "Single")\
                                                   .otherwise("N/A"))
        # clean gender
        df = df.withColumn("cst_gndr",  F.when(F.trim(F.upper(F.col("cst_gndr"))) == "M", "Male")\
                                                   .when(F.trim(F.upper(F.col("cst_gndr"))) == "F", "Female")\
                                                   .otherwise("N/A"))
        #clean dt created
        df = df.filter(~F.col("cst_create_date").isNull())
        print('customer info cleaning successful')
        return df
    except Exceptation as e:
        print('error in the customer info cleaning',e)
        return None
```

#### 3.3 Write back to the containers
```python
# write all transform data as csv to silver dir
def write_to_silver(df,table_name):
    try:
        df.write.mode("overwrite").format("csv").option("header",True).save(f"/mnt/salesData/silver/{table_name}")
        print(f"{table_name} written to silver")
    except Exception as e:
        print(f"error in writing {table_name} to silver",e)
```
----
### Step 4: Power BI Dashboard
The final dataset stored in the Gold layer of the data lake is used to create an interactive Power BI report. This dashboard helps stakeholders monitor sales performance and uncover insights across product categories and time periods.

#### 4.1 Connect Power BI to Gold Layer
- Open Power BI Desktop
- Click Get Data → Choose:
- Azure Data Lake Storage Gen2 
- Authenticate with your Azure credentials
- Load CSVs from the gold container

#### 4.1 Key Metrics & Visualizations
| Visualization                 | Description                                          |
| ----------------------------- | ---------------------------------------------------- |
| **Total Sales (KPI Card)**    | Shows overall revenue from all orders                |
| **Total Orders (KPI Card)**   | Count of all customer orders processed               |
| **Monthly Sales Trend**       | Line chart showing total sales by month      |
| **Sales by Product Line**     | Pie chart of sales revenue per product line   |
| **Sales by Product Category** | Pie chart breaking down revenue by category |

![product_report_dashboard](https://github.com/user-attachments/assets/e7105204-b68c-4f84-a1ef-e864676d339f)

---
### Final Output
- Transformed and cleansed data is available in the Silver container
- Aggregated data is written to the Gold container
- Data from the Gold layer can be connected to Power BI for dashboard creation
---
### Future Enhancements
- Add Delta Lake for ACID transactions and schema enforcement
- Integrate CI/CD using Azure DevOps pipelines
- Implement data quality validation using tools like Great Expectations
- Add alerting and monitoring with Azure Monitor or Log Analytics


