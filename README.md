# Azure-sales-data-engineering
This project is a comprehensive end-to-end data engineering project using Azure services to ingest, transform, and analyze product sales data. The solution implements a Bronze-Silver-Gold architecture using Azure Data Factory, Data Lake Storage Gen2, and Databricks for scalable ETL processing.

---

## Project Overview

This project addresses a critical business scenario by building a scalable data pipeline on Azure. The pipeline extracts product sales data from a Google Drive folder, ingests it into Azure Data Lake Storage Gen2 using Azure Data Factory, and performs transformations using Azure Databricks. The final insights are stored in a Gold layer for visualization using Power BI.

---

## 🧱 Solution Architecture


---

## ⚙️ Technology Stack

| Component              | Purpose                                      |
|------------------------|----------------------------------------------|
| **Azure Data Factory** | Ingests data from Google Drive to ADLS Gen2 |
| **Azure Data Lake Storage Gen2** | Stores raw (bronze), cleaned (silver), and final (gold) data |
| **Azure Databricks**   | Performs data transformation and aggregation |
| **Google Drive**       | Source of raw sales data (CSV)              |

---

## 🧪 Setup Instructions

### 🔐 Prerequisites

- An active **Azure subscription**
- Access to **Google Drive** containing sales data (CSV files)
- Installed: Power BI Desktop (optional)

---

### 🧰 Step 1: Azure Environment Setup

#### 1.1 Create a Resource Group

- Sign in to the [Azure Portal](https://portal.azure.com/).
- Navigate to **Resource Groups** and click **+ Create**.
- Provide a name like `sales-data-rg` and choose a region (e.g., East US).
- Click **Review + Create**, then **Create**.

#### 1.2 Provision Required Azure Services

Use the Azure Portal to manually create the following resources:

- **Azure Storage Account with Hierarchical Namespace enabled**  
  - Go to **Storage accounts** → **+ Create**.  
  - Enable **Data Lake Storage Gen2** by turning on *Hierarchical namespace*.  
  - After creating the account, create the following containers under *Containers*:
    - `bronze` (raw data)
    - `silver` (cleaned data)
    - `gold` (aggregated/final data)

- **Azure Data Factory**
  - Go to **Data factories** → **+ Create**.
  - Choose the same resource group, region, and give it a unique name.
  - Once deployed, access the ADF Studio to create your pipelines.

- **Azure Databricks**
  - Go to **Azure Databricks** → **+ Create**.
  - Choose the same resource group and region.
  - After deployment, launch the workspace and create a cluster.

---

### 📥 Step 2: Data Ingestion

- **Create a pipeline in Azure Data Factory**:
  - Source: HTTP or REST connector pointing to Google Drive link
  - Sink: ADLS Gen2 bronze container

- Schedule the pipeline using a **Trigger** to run every 24 hours

---

### 🧹 Step 3: Data Transformation (Azure Databricks)

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



