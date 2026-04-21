# PySpark E-Commerce Sales Data Pipeline



## Overview



This project demonstrates a beginner to intermediate data engineering pipeline using Apache Spark (PySpark). The pipeline ingests raw e-commerce data, cleaning and transformations, generates business insights, and outputs processed datasets.



---



## Tech Stack



* Python
* PySpark
* Pandas
* Apache Spark (local mode)



---



## Pipeline Workflow



### 1. Data Ingestion

* Load raw CSV data into a Spark DataFrame
* Infer schema and validate structure



### 2. Data Cleaning

* remove duplicate records
* Drop rows with missing critical values
* Filter invalid numerical data (price, quantity)
* Convert date fields to proper format



### 3. Transformation

* Create 'revenue' column (price \* quantity)
* Extract year and month from order date



### 4. Aggregations / Insights

* Top products by revenue
* Sales by region
* Monthly revenue trends



### 5. Output

* Cleaned dataset exported to CSV
* Aggregated insights exported to CSV



> Note: Pandas fallback was used for output due to Windows file system compatibility with Spark.



---



## Project Structure



spark-sales-pipeline/

├── data/

├── output/

├── src/

├── requirements.txt

└── README.md



---



## How to Run



```bash

pip install -r requirements.txt

python src/sales\\\_pipeline.py
```

