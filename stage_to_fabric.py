import os
import pandas as pd
import mysql.connector

# 1. Establish database connection to your local MySQL instance
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",          # Replace with your MySQL username if different
        password="your_password",  # REPLACE WITH YOUR ACTUAL MYSQL PASSWORD
        database="roswell_revcycle_demo"
    )
    print("Successfully connected to local MySQL database.")
except mysql.connector.Error as err:
    print(f"Connection Error: {err}")
    exit()

# 2. Define the extraction query targeting High-Risk Claims
# (This pulls the exact high-value logic you verified in your SQL Workbench)
extraction_query = """    
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
    FROM v_silver_claims_analytics
    WHERE 
        (Is_Unpaid_Leakage = 'True' AND Gross_Charges >= 500.00)
        OR 
        (Adjustment_Rate_Pct >= 80.00 AND Gross_Charges >= 1000.00)
    ORDER BY Gross_Charges DESC;
"""

# 3. EXTRACT: Load data directly into a pandas DataFrame
print("Extracting revenue data from view...")
df_high_risk = pd.read_sql(extraction_query, conn)

# Close connection
conn.close()

# 4. TRANSFORM / PREP: Ensure strict data types for cloud staging
# (Converting dates to string or datetime object, handling nulls)
df_high_risk['Service_Date'] = df_high_risk['Service_Date'].astype(str)
df_high_risk['Gross_Charges'] = df_high_risk['Gross_Charges'].astype(float)
df_high_risk['Actual_Paid_Amount'] = df_high_risk['Actual_Paid_Amount'].astype(float)

print(f"Extraction successful. Found {len(df_high_risk)} high-risk claims records.")

# 5. LOAD: Write the DataFrame to a local compressed Parquet file
# This mirrors a local gateway staging file prior to a Fabric Lakehouse push
output_dir = "staged_fabric_files"
os.makedirs(output_dir, exist_ok=True)
parquet_file_path = os.path.join(output_dir, "high_risk_claims_silver.parquet")

print("Writing data to compressed Parquet format...")
df_high_risk.to_parquet(
    parquet_file_path, 
    engine='pyarrow', 
    compression='snappy', 
    index=False
)

print("Success! File successfully staged at: {os.path.abspath(parquet_file_path)}")
print("This Parquet file is now structurally optimized for immediate ingestion into a Microsoft Fabric Lakehouse.")
