# PySpark Sales ETL Pipeline

## Project Overview

This project demonstrates an end-to-end ETL pipeline using PySpark and Databricks Delta Lake.

The pipeline follows the Medallion Architecture:

Raw Data → Bronze → Silver → Gold

## Technologies

- Python
- PySpark
- Databricks
- Delta Lake
- SQL

## Architecture

Raw Sales Data
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer

## Bronze Layer

Raw sales data is ingested into a Delta table:

- bronze_sales

## Silver Layer

The Silver layer performs:

- Date conversion
- Duplicate removal
- Null handling
- Total sales calculation

Output:

- silver_sales

## Gold Layer

The Gold layer creates business-level aggregations:

- Sales by category
- Sales by region
- Sales by product

Tables:

- gold_sales_by_category
- gold_sales_by_region
- gold_sales_by_product

## Key PySpark Operations

- createDataFrame()
- withColumn()
- to_date()
- dropDuplicates()
- dropna()
- groupBy()
- agg()

## Project Architecture

This project demonstrates how raw sales data can be transformed through multiple data processing layers to produce business-ready datasets.
