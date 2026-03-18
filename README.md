# azure-energy-pipeline

# Azure Energy & CO₂ Analytics Pipeline

## Overview
This project demonstrates an end-to-end data engineering pipeline on Azure to process raw energy and environmental data into business-ready insights.

The pipeline transforms raw sensor data into daily building-level metrics such as energy consumption, CO₂ emissions, temperature, and occupancy.

---

## Problem
Raw energy data is often inconsistent, contains missing values, and is not directly usable for analytics or decision-making.

---

## Solution
Built a scalable pipeline using Azure services to:
- ingest raw data into a data lake
- clean and validate data
- transform it into structured datasets
- aggregate it into meaningful business metrics
- automate the entire workflow

---

## Architecture
ADLS (Raw) → Databricks (Processing) → ADLS (Silver/Gold) → Synapse SQL (Query)  
Azure Data Factory is used for orchestration.

---

## Tech Stack
- Azure Data Lake Storage Gen2
- Azure Databricks (PySpark)
- Azure Data Factory
- Azure Synapse Analytics (Serverless SQL)

---

## Key Features
- Data Quality Handling (nulls, invalid values)
- Medallion Architecture (Raw → Silver → Gold)
- Partitioning by `reading_date` for performance optimization
- Incremental Load using partition-based overwrite
- Automated pipeline using Azure Data Factory

---

## Data Flow
1. Raw CSV data ingested into ADLS (raw layer)
2. Databricks processes and cleans data → stored in silver layer
3. Aggregations applied to create daily building-level metrics → stored in gold layer
4. Data queried using Synapse Serverless SQL
5. Entire pipeline orchestrated using Azure Data Factory

---

## Output
The final dataset provides:
- total energy consumption (kWh)
- total gas usage
- CO₂ emissions
- average temperature
- average occupancy
- per building, per day

---

## Project Structure


├── notebooks/
│ └── nb_energy_pipeline.py
├── data/
│ └── sample_data.csv
├── architecture/
│ └── diagram.png
├── README.md


## Future Improvements
- Add monitoring and logging
- Implement parameterized pipelines
- Extend to real-time streaming data
