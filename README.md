### Bank-Transaction-Pattern-Detection-System.

This project implements an end-to-end detection system for identifying notable transaction behaviors across customers and merchants using large-scale data. The system uses:

- **Mechanism X**: Splits and uploads transaction data into chunks.
- **Mechanism Y**: Detects suspicious transaction patterns using PySpark (AWS Glue) and stores alerts in S3.
  
It involves real-time-like streaming of data via S3, pattern analysis using Glue jobs, and output batching. PostgreSQL is used for intermediate metadata storage.

---

## 🧱 Block Diagram

![Block Diagram](Images/659B2776-690B-403B-9D19-3CAD26702D11_1_201_a.jpeg)

**Description**:  
The block diagram outlines the overall architecture of the system:

- **Google Drive** → Hosts the original `transactions.csv` and `CustomerImportance.csv`.
- **Mechanism X** (in Jupyter Notebook) → Reads the CSV, splits it into 10,000-row chunks, and uploads to S3 in Parquet format.
- **Mechanism Y** (Glue PySpark Script) → Watches for new chunks in S3, applies detection logic using the importance data, and writes batched results (50 per file) back to another S3 folder.
- **PostgreSQL** → Used to optionally track job metadata/state.
- **CloudWatch Logs** → Monitors Glue script execution.

---

## ⚙️ AWS Glue Job Logs

![Glue Logs](Images/AB8E92CF-E570-4B6E-9336-1A1282E9A42D_1_201_a.jpeg)

**Description**:  
These logs provide insights into:

- When chunks are picked up by Glue
- Execution flow and time per detection step
- Any failures or data anomalies encountered
- Helpful for debugging and optimization

---

## 📂 Sample S3 Input Chunks

![S3 Input Chunks](Images/7AFE4E03-E3D4-49F3-B601-C8769ADC1607_1_201_a.jpeg)

**Description**:  
Each file (e.g., `chunk_00001.parquet`) contains exactly 10,000 transactions read from the original CSV. These are:

- In Parquet format for optimal performance in Spark
- Continuously uploaded by Mechanism X
- Monitored by Glue job (Mechanism Y) in real time

---
## Implementation of glue pyspark
![Implementation](Images/0E21152F-33FA-40DF-8FD6-5A0BC37302D1_1_201_a.jpeg)

Reads those chunks from S3, detects 3 patterns, and writes output (50 detections/file) to S3.

Patterns to detect:

-PatId1 (UPGRADE): Top 1% customers by transactions with a merchant, but bottom 1% weight from CustomerImportance.csv. Only after merchant has >50K transactions.

-PatId2 (CHILD): Average txn < ₹23 for a customer with a merchant and count ≥ 80.

-PatId3 (DEI-NEEDED): Female customers < Male customers overall, but female count > 100.
## ✅ Output Detection Files

![Output Files](Images/007963A5-AD78-4B82-9BAD-12B36EFF4717_1_201_a.jpeg

**Description**:  
Each output file contains **50 detected patterns**, named in the format `batch_N/`:

- Each row corresponds to a detection with fields like `step`, `customer`, `amount`, `pattern`
- The `pattern` field includes one of the pattern IDs (PatId1, PatId2, PatId3)
- These outputs are written by Glue and saved back to a separate S3 path
- Ideal for real-time review or analytics dashboards

---

✅ This project showcases how distributed systems, cloud storage, and big data tools like AWS Glue and S3 can be used to automate detection pipelines in a scalable way.
##Downloadale otuput ZIP file : https://bank-transactions-pipeline.s3.amazonaws.com/output/zipped_files.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVT7PC7SXRW6VE6JH%2F20250604%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Date=20250604T073027Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=c110ba98a5334c9d57c62efa60a9748d9d0c0320171c8fe8bee86599eec00f72
