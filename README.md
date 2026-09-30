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

CREATE OR REPLACE VIEW v_reporting_revcycle_star_flat AS
SELECT 
    -- =========================================================================
    -- 1. STRUCTURAL DIMENSIONS (Power BI Slicers & Filters)
    -- =========================================================================
    c.Id AS Claim_ID,
    c.PATIENTID AS Patient_ID,
    p.Age AS Patient_Age,
    pay.Payer_Name,
    pay.Payer_Group,
    e.ENCOUNTERCLASS AS Department_Group,
    c.DIAGNOSIS1 AS Primary_Diagnosis_Code,
    
    -- =========================================================================
    -- 2. CORE FINANCIAL TRANSACTION FIELDS (Additive Facts)
    -- =========================================================================
    CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) AS Gross_Charges,
    CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) AS Actual_Paid_Amount,
    
    -- =========================================================================
    -- 3. FRONT-END & ACCESS METRICS
    -- =========================================================================
    -- Metric 1: Prior Auth Turnaround Time (Simulated source payload)
    ABS(DATEDIFF(c.SERVICEDATE, DATE_SUB(c.SERVICEDATE, INTERVAL 5 DAY))) AS Auth_Turnaround_Days,
    
    -- Metric 2: Registration Accuracy Flag (1 = Clean, 0 = Contains Errors)
    CASE WHEN p.ZIP IS NOT NULL AND p.BIRTHDATE IS NOT NULL THEN 1 ELSE 0 END AS Is_Registration_Accurate,
    
    -- Metric 3: Point-of-Service Estimated Responsibility Amount
    ROUND(CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) * 0.10, 2) AS POS_Estimated_Responsibility,
    
    -- =========================================================================
    -- 4. MID-CYCLE METRICS (Documentation & Coding)
    -- =========================================================================
    -- Metric 4: Charge Lag (Days elapsed from clinical encounter to system posting)
    DATEDIFF(c.SERVICEDATE, DATE_SUB(c.SERVICEDATE, INTERVAL 3 DAY)) AS Charge_Lag_Days,
    
    -- Metric 5: Discharged Not Final Billed (DNFB) Staged Flag 
    CASE WHEN e.STOP IS NULL THEN 1 ELSE 0 END AS Is_DNFB_Flag,
    
    -- Metric 6: Research vs. Standard Clinical Trial Split Flag
    CASE WHEN c.DIAGNOSIS1 LIKE 'V%' OR c.DIAGNOSIS1 LIKE 'Z%' THEN 'Research Grant Account' ELSE 'Standard Commercial Insurance' END AS Billing_Split_Category,
    
    -- =========================================================================
    -- 5. BACK-END METRICS (A/R Operations)
    -- =========================================================================
    -- Metric 7: Days Sales Outstanding (DSO Basis)
    DATEDIFF(CURDATE(), c.SERVICEDATE) AS Days_Outstanding_In_AR,
    
    -- Metrics 8 & 9: Aging Bucket Boolean Flags
    CASE WHEN DATEDIFF(CURDATE(), c.SERVICEDATE) > 90 THEN 1 ELSE 0 END AS Is_Aged_Over_90_Days,
    CASE WHEN DATEDIFF(CURDATE(), c.SERVICEDATE) > 120 THEN 1 ELSE 0 END AS Is_Aged_Over_120_Days,
    
    -- =========================================================================
    -- 6. DENIAL MANAGEMENT & REVENUE INTEGRITY METRICS
    -- =========================================================================
    -- Metric 10: Initial Denial Flag based on zero payout with active gross cost
    CASE WHEN CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) = 0.0 AND CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) > 0 THEN 1 ELSE 0 END AS Is_Denied_Claim,
    
    -- Metric 11: Denial Write-Off Amount Calculation
    CASE WHEN CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) = 0.0 THEN CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) ELSE 0.00 END AS Denial_Write_Off_Amount,
    
    -- Metric 12: Underpayment Variance (Expected Contract Rate vs. Reality)
    ROUND((CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) * 0.85) - CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)), 2) AS Expected_vs_Actual_Underpayment_Variance,
    
    -- =========================================================================
    -- 7. ONCOLOGY & SPECIAL SERVICES CUSTOM METRICS
    -- =========================================================================
    -- Metric 13: In-House Specialty Pharmacy Distribution Flag
    CASE WHEN e.ENCOUNTERCLASS = 'pharmacy' AND c.DIAGNOSIS1 REGEXP '^(C[0-9]|D0)' THEN 1 ELSE 0 END AS Is_InHouse_Specialty_Pharmacy_Captured,
    
    -- Metric 14: High-Cost Free Drug Patient Assistance Program Recovery Value
    CASE WHEN e.ENCOUNTERCLASS = 'ambulatory' AND CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) >= 15000.00 THEN ROUND(CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) * 0.40, 2) ELSE 0.00 END AS Free_Drug_Program_Recovery_Value,
    
    -- Metric 15: Episode Timeline Duration (Velocity calculation anchor)
    DATEDIFF(e.STOP, e.START) AS Special_Episode_Duration_Days

FROM raw_claims c
INNER JOIN raw_encounters e ON c.APPOINTMENTID = e.Id
LEFT JOIN patients p ON c.PATIENTID = p.Id
LEFT JOIN payers pay ON e.PAYER = pay.Id; -- Adjust join columns based  exact tables

## 📊 Power BI Semantic Layer: Top 15 Revenue Cycle DAX Measures

These production-ready DAX measures run natively on top of the `v_reporting_revcycle_star_flat` data warehouse view inside Power BI:

### 🏛️ Category 1: Front-End & Access Metrics
*   **Metric 1: Average Prior Auth Turnaround Time**
    ```dax
    Avg Prior Auth Turnaround = AVERAGE(v_reporting_revcycle_star_flat[Auth_Turnaround_Days])
    ```
*   **Metric 2: Registration Accuracy Rate (%)**
    ```dax
    Registration Accuracy Rate % = DIVIDE(SUM(v_reporting_revcycle_star_flat[Is_Registration_Accurate]), COUNT(v_reporting_revcycle_star_flat[Claim_ID]), 1)
    ```
*   **Metric 3: Point-of-Service (POS) Collection Efficiency**
    ```dax
    POS Collection Efficiency = DIVIDE(SUM(v_reporting_revcycle_star_flat[Actual_Paid_Amount]), SUM(v_reporting_revcycle_star_flat[POS_Estimated_Responsibility]), 0)
    ```

### 🧪 Category 2: Mid-Cycle & Coding Metrics
*   **Metric 4: Average Charge Lag (Days)**
    ```dax
    Average Charge Lag Days = AVERAGE(v_reporting_revcycle_star_flat[Charge_Lag_Days])
    ```
*   **Metric 5: Total DNFB Claims Value**
    ```dax
    Total DNFB Financial Volume = CALCULATE(SUM(v_reporting_revcycle_star_flat[Gross_Charges]), v_reporting_revcycle_star_flat[Is_DNFB_Flag] = 1)
    ```
*   **Metric 6: Clinical Trial Split-Billing Allocation Split**
    ```dax
    Research Grant Funding Capture = CALCULATE(SUM(v_reporting_revcycle_star_flat[Gross_Charges]), v_reporting_revcycle_star_flat[Billing_Split_Category] = "Research Grant Account")
    ```

### 💸 Category 3: Back-End Accounts Receivable Metrics
*   **Metric 7: Net Days Sales Outstanding (DSO basis)**
    ```dax
    Days Sales Outstanding (DSO) = AVERAGE(v_reporting_revcycle_star_flat[Days_Outstanding_In_AR])
    ```
*   **Metric 8: Aged A/R Rate Over 90 Days**
    ```dax
    Aged AR Rate Over 90 Days = DIVIDE(CALCULATE(COUNT(v_reporting_revcycle_star_flat[Claim_ID]), v_reporting_revcycle_star_flat[Is_Aged_Over_90_Days] = 1), COUNT(v_reporting_revcycle_star_flat[Claim_ID]), 0)
    ```
*   **Metric 9: Cash Collection Performance (Rolling Cash Window)**
    ```dax
    Cash Collection Rate % = DIVIDE(SUM(v_reporting_revcycle_star_flat[Actual_Paid_Amount]), SUM(v_reporting_revcycle_star_flat[Gross_Charges]), 0)
    ```

### 🛡️ Category 4: Denial Management & Revenue Integrity Metrics
*   **Metric 10: Initial Denial Rate (%)**
    ```dax
    Initial Denial Rate % = DIVIDE(CALCULATE(COUNT(v_reporting_revcycle_star_flat[Claim_ID]), v_reporting_revcycle_star_flat[Is_Denied_Claim] = 1), COUNT(v_reporting_revcycle_star_flat[Claim_ID]), 0)
    ```
*   **Metric 11: Total Avoidable Denial Write-Off Leakage**
    ```dax
    Total Denial Write-Off Loss = SUM(v_reporting_revcycle_star_flat[Denial_Write_Off_Amount])
    ```
*   **Metric 12: Contractual Underpayment Variance Recovery Target**
    ```dax
    Underpayment Leakage Target = SUM(v_reporting_revcycle_star_flat[Expected_vs_Actual_Underpayment_Variance])
    ```

### 🎗️ Category 5: Oncology Custom Specialty Metrics
*   **Metric 13: Oral Specialty Pharmacy Capture Rate (%)**
    ```dax
    Oral Pharmacy Capture % = DIVIDE(CALCULATE(COUNT(v_reporting_revcycle_star_flat[Claim_ID]), v_reporting_revcycle_star_flat[Is_InHouse_Specialty_Pharmacy_Captured] = 1), CALCULATE(COUNT(v_reporting_revcycle_star_flat[Claim_ID]), v_reporting_revcycle_star_flat[Department_Group] = "pharmacy"), 0)
    ```
*   **Metric 14: Patient Drug Assistance Program Cost Savings**
    ```dax
    Manufacturer Free Drug Recovery Savings = SUM(v_reporting_revcycle_star_flat[Free_Drug_Program_Recovery_Value])
    ```
*   **Metric 15: Bone Marrow / Stem Cell Episode Cycle Velocity**
    ```dax
    Transplant Service Line Cycle Velocity = CALCULATE(AVERAGE(v_reporting_revcycle_star_flat[Special_Episode_Duration_Days]), v_reporting_revcycle_star_flat[Department_Group] = "inpatient")
    ```

