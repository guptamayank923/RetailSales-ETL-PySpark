# Retail Sales ETL Pipeline using PySpark

## 📌 Project Overview
This project demonstrates a production-style ETL pipeline built using PySpark.  
The pipeline ingests raw retail transaction data, performs data cleaning and transformations, computes business metrics, and stores the processed data in an optimized format for analytics.

---

## 🚀 Key Features
- Automated ingestion of multiple CSV files
- Data cleaning and validation
- Business transformations (Revenue calculation)
- Aggregations (Monthly revenue, top products)
- Output stored in Parquet format
- Modular and configurable pipeline design

---

## 🧱 Project Architecture
Raw Data (CSV) → PySpark ETL → Processed Data (Parquet)

---

## 📂 Project Structure

Retail_ETL_PySpark/
├─ data/
│ ├─ raw/
│ └─ processed/
├─ scripts/
│ ├─ etl_pipeline.py
│ ├─ config.py
│ └─ utils.py
├─ README.md
├─ requirements.txt
└─ .gitignore


---

## 📊 Dataset
Dataset used: Online Retail Dataset  
Source: UCI Machine Learning Repository  

Download:
https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx  

Note:
Raw data is not included in this repository. Please download the dataset and place CSV files inside `data/raw/`.

---

## ⚙️ Setup Instructions

### 1. Create Virtual Environment

python -m venv Retail_Pyspark_env
Retail_Pyspark_env\Scripts\activate


### 2. Install Dependencies

pip install -r requirements.txt


---

## ▶️ How to Run

python scripts/etl_pipeline.py


---

## 📈 Output
The pipeline generates:

- Monthly Revenue Data
- Top Products by Revenue

Output location:
data/processed/


---

## 🧠 Key Learnings
- Built an end-to-end ETL pipeline using PySpark
- Implemented data transformations and aggregations
- Used Parquet format for optimized storage
- Simulated real-world multi-file ingestion

---

## 💼 Resume Highlight
Built a production-style ETL pipeline using PySpark to process retail transaction data, perform transformations, and generate analytics-ready datasets.

---

## 🔮 Future Enhancements
- Add logging and monitoring
- Integrate with Azure Data Lake
- Orchestrate pipeline using Azure Data Factory
- Implement incremental data loading

---