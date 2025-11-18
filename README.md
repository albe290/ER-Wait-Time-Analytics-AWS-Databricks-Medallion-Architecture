
---

# 🏥 **ER Wait-Time Analytics (AWS + Databricks | Medallion Architecture)**

A Production-Ready Healthcare Data Engineering Pipeline

---

## ⭐ **Executive Summary**

This project implements a **full-scale, production-style Lakehouse pipeline** to analyze Emergency Room (ER) wait-time performance across U.S. hospitals. Using **AWS S3, Databricks, Delta Lake, Unity Catalog, and IAM**, the solution ingests raw CMS healthcare data, cleans and validates it, and transforms it into **analytics-ready Gold tables** following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline enables hospital leaders and analysts to:

* Compare performance across facilities, states, and conditions
* Identify top-performing and underperforming hospitals
* Detect bottlenecks affecting patient outcomes
* Build dashboards and machine learning models

**Security and governance** were implemented using AWS IAM and Databricks Unity Catalog with external S3 locations, following enterprise least-privilege best practices.

This project demonstrates real-world skills in **cloud engineering, Delta Lake optimization, data modeling, governance, troubleshooting, and healthcare analytics**.

---

# 🔍 **1. Business Problem**

ER wait times critically impact:

* Patient safety & outcomes
* Hospital quality ratings
* Staff workload & scheduling
* State and federal healthcare planning
* Operational and financial performance

Healthcare providers often lack:

* A unified analytics model
* Clean, governed data
* Automated transformation pipelines
* Consistent, trusted datasets for reporting
* Scalable performance for millions of rows

**This project solves these challenges by engineering a modern Lakehouse-based analytics platform.**

---

# 🚀 **2. Project Overview**

This repository contains a complete **Databricks Lakehouse pipeline** built on:

* **AWS S3**: raw + curated storage zones
* **Databricks SQL & PySpark**:  ingestion, transformation, Delta Lake processing
* **Unity Catalog**:  full governance and storage permissions
* **AWS IAM** :secure, least-privilege S3 access
* **Delta Lake Medallion Architecture**: Bronze → Silver → Gold
* **SQL warehouse + cluster notebooks**: optimized development workflow

### ⬇ The pipeline delivers insights including:

* ⭐ Top 20 Hospitals in New York
* 🔻 Bottom 20 Hospitals in New York
* 🌎 State-by-state ER performance comparison
* 🩺 Condition-level performance (e.g., Sepsis, ED, Vaccinations)

---

# 🏗 **3. Architecture Diagram**

```
┌──────────────────────────────────────────────┐
│                 SOURCE DATA                  │
│  CMS Timely & Effective Care Dataset (CSV)   │
│  Stored in AWS S3 (Raw Zone)                 │
└──────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                       │
│  • Databricks reads raw CSV directly from AWS S3         │
│  • SQL / CTAS / COPY INTO ingestion paths                │
│  • Unity Catalog governs access using Storage Credentials │
└──────────────────────────────────────────────────────────┘
                      │
                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│                               BRONZE LAYER                             │
│------------------------------------------------------------------------│
│  • Raw, unmodified hospital ER data (Delta)                            │
│  • Immutable audit table                                               │
│  • s3://bucket/raw/                                                    │
└────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│                               SILVER LAYER                             │
│------------------------------------------------------------------------│
│  • Cleaned & standardized schema                                       │
│  • Renaming, trimming, null removal, type enforcement                  │
│  • Partitioned by state                                                │
│  • s3://bucket/silver/                                                 │
└────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│                                 GOLD LAYER                             │
│------------------------------------------------------------------------│
│  • Analytics-ready hospital scoring models                              │
│  • Aggregated metrics: avg score by hospital/state/condition           │
│  • State partitioning for performance                                  │
│  • s3://bucket/gold/                                                   │
└────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────────────────┐
│                     CONSUMPTION LAYER                          │
│----------------------------------------------------------------│
│  • Databricks SQL visualizations                               │
│  • Top 20 / Bottom 20 dashboards                               │
│  • State and condition analytics                               │
│  • Future: Power BI / Lakeview                                 │
└───────────────────────────────────────────────────────────────┘
```

---

# 🧱 **4. Medallion Architecture Implementation**

---

## 🥉 **Bronze - Raw Structured Ingestion**

* Loaded CSV directly from AWS S3
* No transformation
* Schema preserved as `_c0, _c1, …`
* ACID Delta table for audit reliability

**SQL Example:**

```sql
DROP TABLE IF EXISTS bronze_er_wait_times;

CREATE TABLE IF NOT EXISTS bronze_er_wait_times
USING DELTA
AS
SELECT
  _c0  AS facility_id,
  _c1  AS facility_name,
  _c2  AS address,
  _c3  AS city_town,
  _c4  AS state,
  _c5  AS zip_code,
  _c6  AS county_parish,
  _c7  AS telephone_number,
  _c8  AS condition_name,
  _c9  AS measure_id,
  _c10 AS measure_name,
  _c11 AS score,
  _c12 AS sample,
  _c13 AS footnote,
  _c14 AS start_date,
  _c15 AS end_date
FROM csv.`s3://hospital-wait-times-bucket-usw2/bronze/raw/Timely_and_Effective_Care-Hospital.csv`;
```
<img width="2524" height="829" alt="image" src="https://github.com/user-attachments/assets/0088647a-4940-4bd1-a799-a1a4425c0244" />

---

## 🥈 **Silver - Clean, Validated, Modeled Data**

* Removed nulls + duplicates
* Standardized and trimmed text fields
* Casted numeric fields
* Casted dates into `DATE` type
* **Partitioned by state**

**SQL Example:**

```sql
CREATE TABLE silver_er_wait_times
USING DELTA 
PARTITIONED BY (state)
AS 
SELECT 
    TRIM(facility_id)       AS facility_id,
    TRIM(facility_name)     AS facility_name,
    TRIM(address)           AS address,
    TRIM(city_town)         AS city_town,
    TRIM(state)             AS state,
    TRIM(zip_code)          AS zip_code,
    TRIM(county_parish)     AS wait_time,
    TRIM(telephone_number)  AS telephone_number,
    TRIM(condition_name)    AS condition_name,
    TRIM(measure_id)        AS measure_id,
    TRIM(measure_name)      AS measure_name,
    TRY_CAST(score AS INT)  AS score,
    TRY_CAST(sample AS INT) AS sample,
    TRIM(footnote)          AS footnote,
    try_to_date(start_date, 'MM/dd/yyyy') AS start_date,
    try_to_date(end_date, 'MM/dd/yyyy')   AS end_date
FROM bronze_er_wait_times
WHERE facility_id IS NOT NULL;
```
<img width="2533" height="1131" alt="image" src="https://github.com/user-attachments/assets/aee99920-2eee-487b-8774-de9310b836c1" />

---

## 🥇 **Gold - Analytics & BI Layer**

### **Gold #1: Top 20 NY Hospitals**

```sql
SELECT facility_name, state, condition_name, avg_score
FROM gold_er_wait_hospital_summary
WHERE state = 'NY'
ORDER BY avg_score DESC
LIMIT 20;
```
<img width="2530" height="1249" alt="image" src="https://github.com/user-attachments/assets/c759a55d-e68e-4ae8-b18a-212980eb7f69" />

### **Gold #2: Bottom 20 NY Hospitals**

```sql
SELECT facility_name, avg_score
FROM gold_er_wait_hospital_summary
WHERE state = 'NY'
ORDER BY avg_score ASC
LIMIT 20;
```
<img width="2533" height="1222" alt="image" src="https://github.com/user-attachments/assets/bff33f02-737b-4ff6-b676-af7db7edac6a" />

### **Gold #3: State-Level Comparison**

```sql
SELECT state, ROUND(AVG(avg_score), 2) AS avg_state_score
FROM gold_er_wait_hospital_summary
GROUP BY state
ORDER BY avg_state_score DESC;
```
<img width="2512" height="1206" alt="image" src="https://github.com/user-attachments/assets/e0599fc4-bfa6-44cb-aa5c-3bf5b449a690" />

### **Gold #4: Condition-Level Quality**

```sql
SELECT condition_name,
       AVG(record_count) AS avg_condition_score
FROM gold_er_condition_performance
GROUP BY condition_name
ORDER BY avg_condition_score DESC;
```
<img width="2542" height="982" alt="image" src="https://github.com/user-attachments/assets/4c2d20a9-d793-4cec-bfae-b8d6b6d3862a" />

---

# 🧠 **5. Why OPTIONS() Was Not Required**

Databricks only needs `OPTIONS()` for **unstructured or semi-structured** data such as:

* JSON
* XML
* Multi-line text
* Event logs
* Nested formats

The CMS dataset is:

* Clean
* Structured
* Column-consistent CSV

Therefore:

* Headers were parsed automatically
* Schema was inferred correctly
* No special CSV options were necessary

This reflects **real production batch ingestion**.

---

# 🔐 **6. IAM + Unity Catalog Troubleshooting**

This project includes full cloud governance:

### ✔ IAM Role with External ID

### ✔ Least-privilege S3 policies

### ✔ Databricks Storage Credential

### ✔ External Locations for raw/silver/gold

### ✔ Attached to Unity Catalog Metastore

### ✔ Validated with `DESCRIBE STORAGE CREDENTIAL`

Common issues resolved:

* PERMISSION_DENIED
* Missing external ID
* Wrong trust policy
* SQL Warehouse auto-start credit burn

This demonstrates **true production troubleshooting skills.**
https://github.com/albe290/healthcare-data-engineering-troubleshooting
---

# 📊 **7. Visualizations**

### Top 20 Hospitals

<img width="1735" height="739" alt="image" src="https://github.com/user-attachments/assets/0d10db7d-e968-47f6-8bd3-ec1de0de6ea4" />


### State Comparison

<img width="1728" height="745" alt="image" src="https://github.com/user-attachments/assets/bc8b674c-d1f6-41db-8cd0-92d4688c37f0" />


### Condition-Level Quality

<img width="1735" height="741" alt="image" src="https://github.com/user-attachments/assets/61b5ab35-3f66-429a-8356-ed071b09dcc5" />


---

# 📡 **8. Dataset Source**

**Centers for Medicare & Medicaid Services (CMS)**
Timely & Effective Care — Hospital Dataset
[https://data.cms.gov/](https://data.cms.gov/)

---

# 💡 **9. Business Value Delivered**

This pipeline helps:

### ✔ Hospital Executives

Optimize ER operations

### ✔ State-Level Leaders

Allocate resources fairly

### ✔ BI & Analytics Teams

Build dashboards + KPIs

### ✔ Data Teams

Use standardized data for ML

---

# 🛠 **10. Tech Stack**

* Databricks SQL
* PySpark
* Delta Lake
* AWS S3
* AWS IAM
* Unity Catalog
* GitHub

---

# 🧭 **11. Future Enhancements**

* Streaming via Auto Loader
* Lakeview Dashboards
* ML model predicting ER performance
* Databricks Workflows orchestration
* Data quality rules (Expectations)
* Alerting + Monitoring

---

# 🏁 **12. Conclusion**

This project demonstrates:

* Real cloud engineering
* Production data modeling
* Governance + IAM mastery
* Delta Lake optimization
* Analytics engineering
* Healthcare domain knowledge

A complete, production-ready Lakehouse pipeline.

---

# 📁 **Recommended GitHub Repository Structure**

```
er-wait-time-analytics/
│
├── README.md
├── LICENSE
│
├── architecture/
│   ├── ER_Wait_Time_Architecture.png
│   ├── ER_Wait_Time_Horizontal_Banner.png
│
├── bronze/
│   ├── create_bronze_table.sql
│   ├── bronze_sample_output.png
│
├── silver/
│   ├── create_silver_table.sql
│   ├── silver_data_preview.png
│
├── gold/
│   ├── gold_hospital_summary.sql
│   ├── gold_state_summary.sql
│   ├── gold_condition_summary.sql
│   ├── gold_top20.png
│   ├── gold_bottom20.png
│   ├── gold_state_chart.png
│
├── screenshots/
│   ├── sql_editor_queries/
│   ├── permissions_setup/
│   ├── iam_role_troubleshooting/
│
├── data/
│   └── placeholder.txt   # dataset too large to upload
│
└── notebooks/
    ├── bronze_ingestion.dbc
    ├── silver_transformations.dbc
    ├── gold_analytics.dbc
```

---




