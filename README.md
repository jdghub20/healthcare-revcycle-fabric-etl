# Healthcare Revenue Cycle ETL Pipeline: On-Premises to Microsoft Fabric

## 📌 Project Overview
This repository contains an end-to-end Revenue Cycle (Rev Cycle) ETL pipeline designed to simulate migrating and mapping legacy hospital billing data into a modern cloud data environment like **Microsoft Fabric**. 

Using open-source, HIPAA-compliant healthcare data from **Synthea**, this project replicates the challenge of extracting relational financial transactions, executing complex business logic transformation rules, and optimizing the output into compressed **Delta/Parquet formats** required for advanced cloud analytics and executive dashboard reporting.

---

## 🏗️ Architecture Workflow
1. **Extract (Relational Storage):** Raw billing records and clinical events are hosted in a local **MySQL** instance, mirroring typical legacy enterprise setups (e.g., Athena IDX / Meditech).
2. **Transform (Data Modeling & Logic):** A custom SQL engine maps relational fields across tables to calculate core healthcare KPIs including *Gross Charges*, *Contractual Adjustments*, *Adjustment Rate Percentages*, and *Unpaid Revenue Leakage*.
3. **Load (Cloud Staging Preparation):** A **Python/PySpark** automation script connects to the data layer, extracts high-financial-risk anomalies, and serializes the dataset into an optimized **Snappy-Compressed Parquet file** optimized for direct ingestion into a **Microsoft Fabric Lakehouse**.

---

## 💻 Tech Stack
* **Database Engine:** MySQL 8.0+
* **Programming Languages:** SQL, Python 3.x
* **Core Libraries:** `pandas`, `mysql-connector-python`, `pyarrow`
* **Target Environment Platform:** Microsoft Fabric (Delta Lake / Lakehouse optimized)

---

## 📂 Codebase Breakdown

### 1. Data Mapping & Relational Transformation Layer (`transform_views.sql`)
This script resolves structural discrepancies in raw source files by mapping encounter-level pricing logic to claims-level clinical diagnosis variables, generating a unified "Silver Layer" view.

```sql
CREATE OR REPLACE VIEW v_silver_claims_analytics AS
SELECT 
    c.Id AS Claim_ID,
    c.PATIENTID AS Patient_ID,
    e.ENCOUNTERCLASS AS Department_Group,
    c.SERVICEDATE AS Service_Date,
    CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) AS Gross_Charges,
    CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) AS Actual_Paid_Amount,
    ROUND(CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) - CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)), 2) AS Contractual_Adjustment,
    CASE 
        WHEN CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) = 0.0 AND CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) > 0 THEN 'True'
        ELSE 'False'
    END AS Is_Unpaid_Leakage,
    CASE 
        WHEN CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) > 0 
        THEN ROUND(( (CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) - CAST(e.PAYER_COVERAGE AS DECIMAL(10,2))) / CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) ) * 100, 2)
        ELSE 0.00
    END AS Adjustment_Rate_Pct,
    c.DIAGNOSIS1 AS Primary_Diagnosis_Code
FROM raw_claims c
INNER JOIN raw_encounters e ON c.APPOINTMENTID = e.Id;
```

### 2. Cloud Staging Pipeline Automation (`stage_to_fabric.py`)
This pipeline isolates high-risk financial anomalies (unpaid high-value claims or excessive write-offs) and converts them to Parquet format to prepare them for seamless, high-speed ingestion over a Fabric On-Premises Data Gateway.

```python
import os
import pandas as pd
import mysql.connector

# Connect to legacy database layer
conn = mysql.connector.connect(host="localhost", user="root", password="YOUR_PASSWORD", database="roswell_revcycle_demo")

# Isolate financial anomalies & operational leakage
extraction_query = """    
    SELECT * FROM v_silver_claims_analytics
    WHERE (Is_Unpaid_Leakage = 'True' AND Gross_Charges >= 500.00)
       OR (Adjustment_Rate_Pct >= 80.00 AND Gross_Charges >= 1000.00)
    ORDER BY Gross_Charges DESC;
"""

df_high_risk = pd.read_sql(extraction_query, conn)
conn.close()

# Format and serialize to compressed Parquet format for Delta Lake compatibility
output_path = "staged_fabric_files/high_risk_claims_silver.parquet"
df_high_risk.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
print(f"Success! Optimized file staged for Microsoft Fabric Lakehouse landing.")
```

---

## 🎯 Key Business Metrics Tracked
* **Gross Charges vs. Actual Paid:** Tracks top-line billing against actual payer reimbursement.
* **Contractual Adjustments:** Identifies the exact revenue written off per department/clinical category.
* **Unpaid Leakage Flag:** Instantly isolates complex claims that resulted in a $0 payout, streamlining the workflow for medical necessity and clinical trial denial audits.
