# 🛒 End-to-End E-Commerce Data Engineering & Analytics Platform

An enterprise-grade, end-to-end data platform built to simulate, ingest, orchestrate, store, and analyze **5,000,000+ realistic e-commerce user behavior events**.

This project demonstrates advanced data engineering practices including **probabilistic Markov chain synthetic data modeling**, **automated pipeline orchestration with Apache Airflow**, **cloud data warehousing and customer analytics on Google BigQuery**, **distributed big data processing & data quality frameworks on Apache Spark (PySpark)**, and **real-time event streaming with Apache Kafka**.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph DataGen["1. Data Modeling & Generation"]
        A["Markov Chain Engine<br/>(Zipf Distribution, 30-Day Window)"] -->|Streaming JSONL| B["5M+ E-Commerce Events<br/>(events_5m.jsonl)"]
        A -.->|Data Contract| DB["PostgreSQL / Supabase<br/>(CHECK Constraints)"]
    end

    subgraph Streaming["2. Streaming Ingestion"]
        B -.->|Producer| K1["Apache Kafka Broker<br/>(ecommerce-events / 3 Partitions)"]
        K1 -->|Consumer| K2["Python Consumer &<br/>Spark-SQL-Kafka Connector"]
    end

    subgraph Orchestration["3. Pipeline Orchestration"]
        C["Apache Airflow (Docker)<br/>DAG: ecommerce_analytics_pipeline"]
        C --> D["Task 1: prepare_data"]
        D --> E["Task 2: validate_data (retries=2)"]
        E --> F["Task 3: analyze_data"]
    end

    subgraph DWH["4. Cloud Data Warehouse & Analytics"]
        B -->|split_events.py (11 Parts)| G["Google BigQuery"]
        G --> H["Deduplication<br/>(events_clean)"]
        H --> I["Funnel & Product KPI Analytics"]
        H --> J["RFM Customer Segmentation<br/>(51.8k Buyers)"]
    end

    subgraph Distributed["5. Distributed Processing"]
        B --> K["Apache Spark (PySpark / WSL2)"]
        K --> L["Partition & Shuffle Optimization<br/>(repartition vs coalesce)"]
        K --> M["Broadcast Hash Join &<br/>Data Skew Mitigation (HOT_USER)"]
        K --> N["5-Dimensional Data Quality Framework"]
        K --> O["Customer Lifetime Value (CLV) &<br/>Cohort Retention Analysis"]
    end
```

---

## 🛠️ Tech Stack & Capabilities

| Layer | Technologies | Responsibilities |
| :--- | :--- | :--- |
| **Data Generation & Modeling** | Python, Markov Chains, Zipf's Law | Event-driven simulation, memory-safe JSONL streaming (100K, 1M, 5M). |
| **Relational Data Contract** | PostgreSQL, Supabase | Strict schema enforcement, table relationships, and CHECK constraint tests. |
| **Pipeline Orchestration** | Apache Airflow, Docker, Celery, Redis | Automated batch pipeline with retry/backoff policies and monitoring. |
| **Cloud Data Warehouse** | Google Cloud BigQuery, SQL | Deduplication, funnel analysis, search intent, and RFM customer segmentation. |
| **Distributed Big Data Engine** | Apache Spark (PySpark 3.5+ / 4.x) | 12-partition data processing, Broadcast Joins, Data Skew simulation, CLV. |
| **Streaming & Messaging** | Apache Kafka 4.0, Docker | Real-time producer/consumer, partition vs offset analysis, Spark Streaming. |
| **Data Quality & Reliability** | PySpark Data Quality Framework | 5-dimensional validation (Duplicates, Missing, Invalid, Inconsistency, Anomaly). |

---

## 📂 Repository Structure

```plaintext
ecommerce-data-pipeline/
├── data-generation/                   # Markov Chain synthetic data engine
│   ├── generate_events.py             # Memory-safe streaming generator (100k, 1M, 5M)
│   ├── validate_events.py             # Rule-based validation (session timeout, limits)
│   ├── analyze_data.py                # Python-based funnel and KPI baseline analysis
│   ├── analyze_events.py              # Event frequency and session counter utility
│   ├── split_events.py                # Chunks 972 MB file into 11 parts for BigQuery
│   └── sample_events.jsonl            # Git-friendly sample dataset for quick testing
├── orchestration/                     # Workflow automation with Apache Airflow
│   ├── dags/
│   │   └── ecommerce_pipeline.py      # Airflow DAG (prepare -> validate -> analyze)
│   ├── docker-compose.yaml            # Multi-container Airflow setup (Celery + Redis)
│   └── config/                        # Airflow configuration files
├── spark-analytics/                   # PySpark big data jobs & optimization
│   ├── 01_spark_basics.py             # 5M dataset loading, DataFrame schema & actions
│   ├── 02_partition_shuffle_skew.py   # repartition vs coalesce, Broadcast Join, Data Skew
│   ├── 03_customer_analytics_clv_cohort.py # RFM, Historical CLV, and Cohort retention
│   ├── 04_data_quality_checks.py      # 5-Dimensional automated Data Quality audit
│   └── data/                          # Sample data folder for PySpark jobs
├── kafka-streaming/                   # Real-time event streaming pipeline
│   ├── kafka_producer.py              # Event Producer emitting 5 event types
│   ├── kafka_consumer.py              # Consumer logging partitions and offsets
│   └── kafka_spark.py                 # Spark-Kafka connector & partition comparison
├── sql/                               # Production SQL queries and DDL contracts
│   ├── supabase_schema_and_contract.sql # PostgreSQL DDL with strict CHECK constraints
│   ├── bigquery_analytics.sql         # Dedup, Funnel, Product & Search analytics
│   └── bigquery_rfm_segmentation.sql  # User-level RFM scoring and segment assignment
├── .gitignore                         # Protects repository from multi-GB dataset files
├── requirements.txt                   # Python core dependencies
└── README.md                          # Comprehensive project documentation
```

---

## 🔬 Key Engineering Highlights & Methodologies

### 1. Probabilistic Markov Chain Simulation
Unlike static mock data generators, this project implements a dynamic Markov decision process. Each user session begins with `PRODUCT_LIST_VIEW` and transitions based on calibrated e-commerce probabilities:
* `PRODUCT_LIST_VIEW` $\rightarrow$ `SEARCH` (30%), `PRODUCT_DETAIL_VIEW` (50%), `END` (20%)
* `PRODUCT_DETAIL_VIEW` $\rightarrow$ `PRODUCT_DETAIL_VIEW` (20%), `ADD_TO_CART` (8%), `END` (72%)
* `ADD_TO_CART` $\rightarrow$ `PURCHASE` (30%), `PRODUCT_DETAIL_VIEW` (25%), `ADD_TO_CART` (10%), `END` (35%)
* **Zipfian Popularity Distribution**: 300 products mapped via power-law distribution to accurately model realistic long-tail purchasing trends.
* **Temporal Spread**: User initial sessions are dynamically distributed across a 30-day window (`N_DAYS = 30`) to facilitate meaningful DAU (Daily Active Users) and cohort retention tracking.

### 2. Distributed Spark Optimization & Data Skew Experiment
* **Partitioning Strategy**: Evaluated physical execution plans via `.explain("formatted")`. Demonstrated how `repartition(24)` triggers full network shuffle (`RoundRobinPartitioning` & `Exchange`), whereas `coalesce(4)` merges adjacent partitions with zero shuffle overhead.
* **Broadcast Hash Join**: Avoided expensive sort-merge joins when enriching the 5M event table with a 100-item product metadata dimension table via `F.broadcast(products_df)`.
* **Controlled Data Skew**: Synthesized an extreme edge case (100,000 records where 90,000 belonged to `"HOT_USER"`). Analyzed physical plan bottlenecks and key-level redistribution during shuffle stages.

### 3. Kafka vs. Spark Partition Duality
Verified that messaging partitions (Kafka Topic with 3 partitions) serve consumption concurrency, whereas compute partitions (Spark RDD with 6 partitions via `repartition(6)`) govern distributed executor parallelism.

### 4. 5-Dimensional Data Quality (DQ) Audit
Implemented an automated DQ suite across 5,000,002 rows:
1. **Uniqueness**: Verified 0 duplicate event IDs (`total_records == distinct_event_ids`).
2. **Context-Aware Nullability**: Ensured `price` is purposefully `NULL` during `ADD_TO_CART` while strictly required during `PURCHASE`.
3. **Validity**: Confirmed zero negative quantities/prices and strictly enumerated event types.
4. **Referential Consistency**: Verified 100% 1-to-1 mapping between sessions and user IDs (0 orphaned or multi-user sessions).
5. **Anomaly Boundaries**: Enforced upper limits on session lengths (max observed: 15 events; hard limit: 50).

---

## 📊 Summary of Analytics Findings (5M Dataset)

| Metric Category | Metric Name | Value | Technical Context |
| :--- | :--- | :--- | :--- |
| **Scale & Volume** | Total Processed Events | **5,000,002** | 972 MB raw streaming JSONL |
| | Unique Users | **1,487,371** | ~70% 1 session, 20% 2 sessions, 10% 3 sessions |
| | Unique Sessions | **2,082,635** | Avg: 2.40 - 2.62 events/session |
| **Funnel Conversion** | Detail View Rate | **72.98%** | 1,519,859 sessions reached Detail View |
| | Add to Cart Rate | **7.31%** | 152,346 sessions added to cart |
| | Overall Purchase Rate | **2.51%** | 52,260 sessions converted (Benchmark: 2-3%) |
| **Financials** | Total Gross Revenue | **173,928,447.87** | Currency units |
| | Purchasing Users | **51,819** | ~3.48% user conversion rate |
| | Top Selling Product | **Product ID #1** | 309,192 views, 9,301 sales (Volume Leader) |
| | Top Grossing Product | **Product ID #3** | ~16.56M Revenue (High-Ticket Leader) |

### RFM Segmentation Distribution (51,819 Customers)
* 🏆 **Champions**: 9 users
* 💎 **Loyal Customers**: 369 users
* 🌟 **New Customers**: 8,989 users
* ⚠️ **At Risk**: 4,058 users
* 💤 **Lost**: 18,090 users
* 📦 **Other**: 20,304 users

---

## 🚀 Getting Started

### Prerequisites
* Python 3.11+
* Docker & Docker Compose
* Java 17 or 21 (for PySpark)
* (Optional) Google Cloud account for BigQuery Sandbox

### 1. Installation
```bash
git clone https://github.com/your-username/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate Synthetic Events
```bash
cd data-generation
python generate_events.py  # Generates events_100k.jsonl, events_1m.jsonl or events_5m.jsonl
python validate_events.py  # Runs data integrity checks
python analyze_data.py     # Computes funnel metrics
```

### 3. Run Apache Airflow Pipeline
```bash
cd ../orchestration
docker compose up -d
# Access Airflow UI at http://localhost:8080 (Default: airflow / airflow)
# Trigger DAG: ecommerce_analytics_pipeline
```

### 4. Run PySpark Big Data Jobs
```bash
cd ../spark-analytics
python 01_spark_basics.py
python 02_partition_shuffle_skew.py
python 03_customer_analytics_clv_cohort.py
python 04_data_quality_checks.py
```

### 5. Run Kafka Streaming
```bash
# In terminal 1 (Consumer):
cd ../kafka-streaming
python kafka_consumer.py

# In terminal 2 (Producer):
python kafka_producer.py

# In terminal 3 (Spark Streaming reader):
python kafka_spark.py
```

---

## 👩‍💻 Author
**Eda Akkuş**  
*Data Engineering & Analytics Internship Project*

