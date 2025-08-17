# real-estate-etl-pipeline
# 🏡 Big Data ETL and Analytics on Indian Housing Prices

## 📌 Overview
This project demonstrates an **end-to-end Big Data ETL pipeline** for Indian Housing Price data using the **Hadoop Ecosystem**.  
The pipeline ingests raw housing price data, applies **data cleaning and feature engineering**, and stores the transformed data in a **Parquet-based Hive warehouse** for scalable analytics.  
Workflow orchestration and automation are handled using **Apache Airflow**.  

---

## ⚙️ Tech Stack
- **HDFS** → Distributed data storage  
- **Hive** → Data warehouse & external tables for querying  
- **PySpark** → Data cleaning, transformation, and feature engineering  
- **Parquet** → Optimized columnar storage format  
- **Apache Airflow** → Workflow orchestration & scheduling  

---

## 📂 Project Architecture
1. **Raw Data Ingestion**  
   - Load raw CSV files into **HDFS**.  

2. **Hive External Table (Raw Data)**  
   - Create an **external Hive table** pointing to raw data in HDFS.  

3. **Data Cleaning & Feature Engineering (PySpark)**  
   - Handle missing values, outliers, and inconsistent data.  
   - Generate features such as:
   - Fair Market Value / Overpriced Listings
     For each group, it calculates the average price per square foot.
     A property is marked as Overpriced (yes) if its price per SqFt is more than 20% higher than the average of similar properties in the same city and BHK category.
   - Investment Hotspot Detection
   - Affordable Housing Detection


4. **Store in Parquet Format**  
   - Save the transformed data in **Parquet** format (efficient for analytics).  

5. **Hive External Table (Processed Data)**  
   - Create another **Hive external table** pointing to the processed Parquet files.  

6. **Workflow Automation**  
   - Use **Apache Airflow DAGs** to automate the ETL pipeline.  
   - Supports **scheduled execution** and **monitoring**.  

---

## 🛠️ ETL Workflow

```bash
Raw CSV (Local/External Source)
        ↓
      HDFS
        ↓
Hive External Table (Raw)
        ↓
     PySpark
 (Cleaning + Feature Engineering)
        ↓
   Parquet Files
        ↓
Hive External Table (Processed)

