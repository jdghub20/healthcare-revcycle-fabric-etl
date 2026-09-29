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
