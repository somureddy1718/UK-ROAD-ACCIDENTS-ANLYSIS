# UK-ROAD-ACCIDENTS-ANLYSIS
This term project for MSIS 5663 – Advanced Data Wrangling – Spring 2024 focuses on analyzing traffic accidents in the UK using a comprehensive data management approach that spans relational, dimensional, and NoSQL paradigms.

We utilize a real-world dataset from Kaggle—UK Road Safety: Traffic Accidents and Vehicles—to explore how modern data wrangling tools and database systems can be applied to derive meaningful insights from large-scale public safety data.

The project is divided into three main components:

Normalized OLTP Database (Relational)
We designed and implemented a fully normalized relational database in 3NF to store cleaned and structured data from the extracted CSV files. Views and complex SQL queries were developed to enable efficient querying and analysis.

Dimensional OLAP Data Warehouse (Analytical)
We built a star-schema data warehouse using a fact table and multiple dimension tables (including a full Date dimension). Data was extracted from the OLTP database, transformed, and loaded into the OLAP model to enable roll-up, drill-down, and slice-and-dice operations via extended SQL queries.

NoSQL MongoDB Database (Semi-Structured)
The same data was transformed into JSON and loaded into MongoDB collections to explore flexible schema design and perform document-based queries and aggregations.

Python was used throughout for data processing, transformation, and executing queries across all three systems. The project demonstrates a full-stack data pipeline from raw CSV files to relational, analytical, and NoSQL data models.

This project analyzes traffic accident data from the UK using a hybrid data modeling approach. The workflow includes the design and implementation of:

1. A **normalized OLTP database** using extracted accident and vehicle data
2. A **dimensional OLAP data warehouse** for analytical queries
3. A **NoSQL MongoDB database** for semi-structured data querying and aggregation

The project uses Python for data wrangling, query execution, and transformation tasks.

## Dataset

- **Source**: [UK Road Safety: Traffic Accidents](https://www.kaggle.com/datasets/tsiaras/uk-road-safety-accidents-and-vehicles)
- **Files Used**:
  - `Accidents_extract.csv`
  - `Vehicles_extract.csv`
- These are randomly generated subsets from the original dataset using `csvExtract.py`.

## Tasks Breakdown

### Task 1 – OLTP Normalized Database
- Designed normalized schema (3NF) using hybrid top-down/bottom-up approach
- Implemented:
  - Stored procedures to create/drop tables
  - 2 SQL Views
  - 5 SQL Queries:
    - At least 2 use views
    - At least 2 use aggregation
    - At least 3 use joins or sub-queries
- Tools: Python (psycopg2 or cx_Oracle, etc.)

### Task 2 – Dimensional Data Warehouse 
- Designed a star-schema with:
  - One or more fact tables (with additive measures)
  - Several dimension tables (including a Date dimension)
- ETL from OLTP to OLAP using Python
- 5 OLAP-style SQL Queries showcasing:
  - Aggregation
  - ROLLUP
  - DRILLDOWN
- Tools: Python, SQL

### Task 3 – NoSQL MongoDB Database 
- Transformed CSV data to JSON format
- Inserted data into MongoDB collections
- Developed:
  - 3 NoSQL Queries
  - 2 NoSQL Aggregation Pipelines
- Tools: Python (PyMongo)

