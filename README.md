# 🏥 **ER Wait-Time Analytics (AWS + Databricks | Medallion Architecture)**

**A production-style data engineering pipeline for analyzing emergency room performance across U.S. hospitals.**

---

## 🔍 **1. Business Problem**

Emergency Room (ER) wait times directly impact:

* Patient safety
* Hospital operational efficiency
* Staffing workloads
* Quality rankings
* Financial outcomes

Hospitals struggle because their data is often siloed, inconsistent, and difficult to analyze at scale.

**This project delivers a unified, analytics-ready data pipeline for ER performance insights**, enabling administrators to:

* Spot bottlenecks
* Identify high vs. low performing hospitals
* Compare states & conditions
* Improve patient care outcomes

---

## 🚀 **2. Project Overview**

This repository contains a **full Databricks Lakehouse pipeline** built on:

* **AWS S3** – raw + curated storage
* **Databricks** – ingestion, transformation, Delta Lake processing
* **Unity Catalog** – governance, permissions, external locations
* **AWS IAM** – secure, least-privilege data access
* **Delta Lake Medallion Architecture** – Bronze → Silver → Gold

The pipeline transforms CMS hospital quality data into analytics-ready tables powering insights such as:

* Top 20 hospitals in New York
* Bottom 20 hospitals
* State-by-state ER performance
* Condition-level hospital performance

This project reflects real, professional Data Engineering workflows.

---

## 🏗 **3. Architecture Diagram**

A full production architecture with:

* AWS + Databricks official icons
* Medallion layers
* Labeled data flows
* COPY INTO, Delta Optimization, Partitioning



---

## 🧱 **4. Medallion Architecture**

### 🥉 **Bronze: Raw Structured Ingestion**

* Loaded CSVs directly from S3
* Preserved original data (“as-is”)
* No transformations
* Serves as immutable audit layer

**SQL**

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
FROM csv.`s3://hospital-wait-times-bucket-usw2/bronze/raw/Timely_and_Effective_Care-Hospital.csv`
```
<img width="2524" height="829" alt="image" src="https://github.com/user-attachments/assets/465a5c1f-f592-4b67-b977-7572f9132913" />

---

### 🥈 **Silver:  Clean, Validated, Modeled Data**

* Removed duplicates
* Casted columns to correct types
* Standardized naming
* Removed invalid or null rows
* **Partitioned by state** for performance

```sql
USE CATALOG workspace_2801931945210;
USE SCHEMA er_wait_times_dev;

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

-- Cast numeric-ish columns 
TRY_CAST(score AS INT)      AS score,
TRY_CAST(sample AS INT)     AS sample,

TRIM(footnote)              AS footnote,


--Convert dates from text -> DATE 
try_to_date(start_date, 'MM/dd/yyyy') AS start_date,
try_to_date(end_date, 'MM/dd/yyyy')     AS end_date
FROM bronze_er_wait_times
WHERE facility_id IS NOT NULL;
```
<img width="2533" height="1131" alt="image" src="https://github.com/user-attachments/assets/07bca047-55ac-44da-a8dc-1c2b9ed3a501" />
---

### 🥇 **Gold: Analytics & BI Layer**

You produced multiple Gold tables optimized for reporting:

---

#### **Gold #1: Top 20 Hospitals (NY)**

```sql
USE CATALOG workspace_2801931945210;
USE SCHEMA er_wait_times_dev;

SELECT 
  facility_name,
  state,
  condition_name,
  avg_score
  FROM gold_er_wait_hospital_summary
  WHERE state = 'NY'
  ORDER BY avg_score DESC
  LIMIT 20;
```
<img width="2530" height="1249" alt="image" src="https://github.com/user-attachments/assets/224cd30e-6e06-4e57-8e11-9d71853f8aec" />
---

#### **Gold #2: Bottom 20 Hospitals (NY)**

```sql
SELECT 
   facility_name,
   avg_score
   FROM gold_er_wait_hospital_summary
   WHERE state = 'NY'
   ORDER BY avg_score ASC 
   LIMIT 20;
```
<img width="2533" height="1222" alt="image" src="https://github.com/user-attachments/assets/5928d7cd-1247-4faa-9427-3e29cdb58c37" />

---

#### **Gold #3: State-Level Comparison**

```sql
SELECT 
 state,
 ROUND(AVG(avg_score), 2) AS avg_state_score
 FROM gold_er_wait_hospital_summary
 GROUP BY state
 ORDER BY avg_state_score DESC;
```
<img width="2512" height="1206" alt="image" src="https://github.com/user-attachments/assets/f74a5a83-2fbe-43d6-b1d1-d46e1cdbee92" />
---

#### **Gold #4: Condition-Level Quality**

```sql
SELECT condition_name,
       AVG(record_count) AS avg_condition_score
FROM gold_er_condition_performance
GROUP BY condition_name
ORDER BY avg_condition_score DESC;
```
<img width="2542" height="982" alt="image" src="https://github.com/user-attachments/assets/7c980be6-f0cf-417c-aea7-ef6c42b64be9" />
---

## 🧠 **5. Why OPTIONS() Were Not Required**

Databricks requires `OPTIONS()` when loading:

* JSON
* XML
* Nested semi-structured data
* Multi-line records
* Logs or streaming formats

BUT this dataset was:

* A **clean**,
* **Structured**,
* **Well-formatted** CSV
* With a consistent schema

So Databricks automatically:

* Parsed headers
* Inferred schema
* Handled delimiters
* Loaded all rows without additional config

This is exactly how real production batch CSV ingestion behaves.

---

## 🔐 **6. IAM + Unity Catalog Troubleshooting**

This pipeline includes complete AWS–Databricks security configuration:

### ✔ Created an AWS IAM Role

* Added trust policy with Databricks external ID
* Assigned least-privilege S3 permissions:

  * `GetObject` (raw)
  * `Put/DeleteObject` (curated)

### ✔ Added IAM Role as a Databricks Storage Credential

### ✔ Created External Locations

* Raw
* Silver
* Gold

### ✔ Attached External Locations → Catalog → Schema

### ✔ Resolved common errors

* PERMISSION_DENIED
* Missing external ID
* Bad trust relationships
* SQL Warehouse auto-start consuming credits

This documented troubleshooting demonstrates **real-world production experience**.

---

## 📊 **7. Visualizations**

# Top 20 Hospitals
<img width="1735" height="739" alt="image" src="https://github.com/user-attachments/assets/172c5183-cb2d-4049-9153-81f7c35908bc" />

# State Comparison
<img width="1728" height="745" alt="image" src="https://github.com/user-attachments/assets/f5e13631-fbf5-4fcd-9a84-59de0ba26a23" />

# Condition-Level-Quality 
<img width="1735" height="741" alt="image" src="https://github.com/user-attachments/assets/2b5cedde-4b6d-44fa-95bf-c07e71807e76" />


---

## 📡 **8. Dataset Source**

**Source:**
Centers for Medicare & Medicaid Services (CMS)
Timely & Effective Care — Hospital Dataset
📌 [https://data.cms.gov/](https://data.cms.gov/)

This dataset includes metrics on:

* ER wait times
* Condition performance
* Hospital quality
* Facility-level outcomes

---

## 💡 **9. Business Value Delivered**

This pipeline enables stakeholders to:

### ✔ Hospital Executives

Identify bottlenecks + opportunities to improve ER operations.

### ✔ State-Level Leaders

Compare performance across states for resource allocation.

### ✔ Analysts & BI Teams

Build dashboards, KPIs, and trend reporting.

### ✔ Data Teams

Reuse structured Silver/Gold layers for ML, forecasting, and workload optimization.

This model mirrors real analytics engineering pipelines used in healthcare systems today.

---

## 🛠 **10. Tech Stack**

* **Databricks SQL + PySpark**
* **AWS S3**
* **AWS IAM**
* **Delta Lake (ACID, Partitioning, ZORDER)**
* **Unity Catalog**
* **Databricks Notebooks + SQL Editor**
* **GitHub**

---

## 🧭 **11. Future Enhancements**

* Streaming ingestion using **Auto Loader**
* **Lakeview Dashboards** (when credits refresh)
* ML model predicting ER efficiency
* **Airflow / Databricks Workflows** automation
* Data quality rules (expectations)
* Alerting and monitoring

---

## 🏁 **12. Conclusion**

This project demonstrates a **real, production-ready lakehouse pipeline** built on AWS + Databricks using enterprise patterns:

* Cloud engineering
* ETL/ELT design
* Data modeling
* Governance & IAM
* Delta Lake optimization
* Analytics engineering
* Real troubleshooting
* Business storytelling



