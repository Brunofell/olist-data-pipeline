<div align="center">

<img src="https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExcjU1OG1wNnRqN3h5cWJscjZhcmJ6amtocGNneGZoc2I0eW53a2pyaCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/EiX5p2iF1I9b8XecZ3/giphy.gif" width="120" alt="ecommerce animation"/>

# Olist Data Pipeline

**A modular Data Engineering pipeline built with Python and Pandas**  
following the **Medallion Architecture** pattern — Bronze → Silver → Gold

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458?style=flat-square&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Parquet](https://img.shields.io/badge/Storage-Parquet-50C878?style=flat-square)](https://parquet.apache.org)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)

</div>

---

## Overview

This project processes raw **Olist Brazilian E-Commerce** datasets through a layered pipeline, applying data cleaning and transformations at each stage to generate analytical datasets optimized for BI and analytics workloads.

---

## Project Structure

```text
olist-pipeline/
│
├── data/
│   ├── bronze/          # Raw CSV datasets (source of truth)
│   ├── silver/          # Cleaned Parquet datasets
│   └── gold/            # Analytical fact tables
│
├── src/
│   ├── extract/
│   │   └── data_loader.py
│   │
│   ├── transform/
│   │   ├── orders_transformer.py
│   │   ├── order_items_transformer.py
│   │   ├── payments_transformer.py
│   │   ├── reviews_transformer.py
│   │   └── fact_orders_transformer.py
│   │
│   ├── load/
│   │   └── parquet_loader.py
│   │
│   └── utils/
│       └── paths.py
│
├── main.py
├── requirements.txt
└── README.md
```

---

## Medallion Architecture

```
Raw CSV Files  ──▶  Bronze Layer  ──▶  Silver Layer  ──▶  Gold Layer
   (source)          (raw data)        (cleaned)        (analytical)
```

### 🥉 Bronze — Raw Ingestion
Stores original CSV files exactly as received. Immutable, no transformations applied. Acts as the source of truth for all downstream processing.

### 🥈 Silver — Cleaned & Standardized
Applies data quality transformations and outputs Apache Parquet files:

| Transformation | Description |
|---|---|
| Datetime conversion | Parse and standardize all timestamp columns |
| Text normalization | Lowercase, strip, and standardize string fields |
| Duplicate removal | Deduplicate records across all datasets |
| Null handling | Handle and document missing values |
| Data validation | Type checking and domain validation |

**Outputs:** `orders.parquet` · `items.parquet` · `payments.parquet` · `reviews.parquet`

### 🥇 Gold — Business-Ready Analytics
Consolidates Silver tables into a single `fact_orders.parquet` table joining:
- Orders + Order Items + Payments + Customer Reviews

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline orchestration |
| Pandas | Data transformation |
| PyArrow | Parquet read/write support |
| Parquet | Columnar storage format |
| OOP | Modular, maintainable pipeline design |

---

## Getting Started

**1. Clone the repository**
```bash
git clone <your-repository-url>
cd olist-pipeline
```

**2. Create and activate a virtual environment**
```bash
# Linux / Mac
python3 -m venv venv && source venv/bin/activate

# Windows
python -m venv venv && venv\Scripts\activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Add raw datasets**

Place all Olist CSV files inside `data/bronze/`.

**5. Run the pipeline**
```bash
python main.py
```

---

## Key Transformations

**Orders** — Timestamp parsing, duplicate removal, status normalization

**Reviews** — Comment field cleaning, `has_comment` boolean flag, score range validation

**Payments** — Payment type standardization, aggregated amounts per order

---

## Roadmap

- [ ] Logging system
- [ ] Unit testing suite
- [ ] Docker support
- [ ] Apache Airflow orchestration
- [ ] Apache Spark processing
- [ ] dbt transformations
- [ ] Data quality monitoring
- [ ] Cloud storage integration (S3 / GCS)

---

## Dataset

**Olist Brazilian E-Commerce Public Dataset** — available on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

<div align="center">

Made by **Bruno Feliciano Martins**  
Fullstack Developer transitioning into Data Engineering  
Building scalable and maintainable data pipelines with modern practices.

</div>