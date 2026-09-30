CREATE OR REPLACE VIEW roswell_revcycle_demo.v_silver_claims_analytics AS
SELECT 
    -- 1. Structural Mapping & Identification
    c.Id AS Claim_ID,
    c.PATIENTID AS Patient_ID,
    e.ENCOUNTERCLASS AS Department_Group,
    c.SERVICEDATE AS Service_Date,
    
    -- 2. Extracting Financial Metrics from the Encounters table
    CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) AS Gross_Charges,
    CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) AS Actual_Paid_Amount,
    
    -- 3. Revenue Cycle Metrics & Logistical Calculations
    -- Contractual Adjustment = Gross Charges minus what the payer actually covered
    ROUND(CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) - CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)), 2) AS Contractual_Adjustment,
    
    -- Identifying denial risk or completely unpaid claims leakage
    CASE 
        WHEN CAST(e.PAYER_COVERAGE AS DECIMAL(10,2)) = 0.0 AND CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) > 0 THEN 'True'
        ELSE 'False'
    END AS Is_Unpaid_Leakage,
    
    -- Calculating the percentage of the bill written off or discounted
    CASE 
        WHEN CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) > 0 
        THEN ROUND(( (CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) - CAST(e.PAYER_COVERAGE AS DECIMAL(10,2))) / CAST(e.TOTAL_CLAIM_COST AS DECIMAL(10,2)) ) * 100, 2)
        ELSE 0.00
    END AS Adjustment_Rate_Pct,
    
    -- 4. Clinical Context (Crucial for Cancer Centers to group by diagnosis type)
    c.DIAGNOSIS1 AS Primary_Diagnosis_Code

FROM roswell_revcycle_demo.raw_claims c
INNER JOIN roswell_revcycle_demo.raw_encounters e 
    ON c.APPOINTMENTID = e.Id;

-- 1. Verify the view works and pull a high-risk operational sample
SELECT 
    Claim_ID,
    Patient_ID,
    Department_Group,
    Service_Date,
    Gross_Charges,
    Actual_Paid_Amount,
    Contractual_Adjustment,
    Adjustment_Rate_Pct,
    Primary_Diagnosis_Code
FROM roswell_revcycle_demo.v_silver_claims_analytics
WHERE 
    -- Scenario A: Complete insurance denial/leakage on a large bill
    (Is_Unpaid_Leakage = 'True' AND Gross_Charges >= 500.00)
    OR 
    -- Scenario B: Severe revenue write-offs (Payer paid less than 20% of the bill)
    (Adjustment_Rate_Pct >= 80.00 AND Gross_Charges >= 1000.00)
ORDER BY Gross_Charges DESC
LIMIT 10;


-- 2. Optional: Run a quick health-check aggregate summary to prove data integrity
SELECT 
    Department_Group,
    COUNT(*) as Total_Claims,
    ROUND(SUM(Gross_Charges), 2) as Total_Gross_Revenue,
    ROUND(SUM(Actual_Paid_Amount), 2) as Total_Collected,
    ROUND(SUM(Contractual_Adjustment), 2) as Total_Adjustments,
    ROUND(AVG(Adjustment_Rate_Pct), 2) as Avg_Adjustment_Rate_Pct
FROM roswell_revcycle_demo.v_silver_claims_analytics
GROUP BY Department_Group;

-- Top front-end, mid-cycle, back-end, denial, and oncology metrics
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
LEFT JOIN payers pay ON e.PAYER = pay.Id; 
