# Miniclip Data Engineering Task

This project implements a complete real-time data processing pipeline for gaming events (`Init`, `Match`, `InAppPurchase`). It covers data generation, batch processing, real-time data quality enrichment, and real-time aggregation.

## Project Status

| Phase | Component | Status | Description |
|-------|-----------|--------|-------------|
| **Phase I** | **Infrastructure & Batch** | ✅ Completed | Docker environment, Data Generator, Spark Batch Job (Daily Unique Users). |
| **Phase II** | **Real-Time Data Quality** | ✅ Completed | Python service (Kafka Streams style) that cleans and enriches data in real-time. |
| **Phase III** | **Real-Time Aggregation** | ✅ Completed | Spark Structured Streaming job calculating minute-level financial and user metrics. |

---

## 🏗 Architecture

The system follows a **Lambda/Kappa Hybrid Architecture** completely containerized with Docker. It separates the ingestion, cleaning (ETL), and analysis layers (Batch & Streaming).

```mermaid
graph LR
    subgraph Source
        GEN[Data Generator<br/>(Python)]
    end

    subgraph "Ingestion & Data Quality Layer"
        RAW[("Kafka Topic:<br/>events-raw")]
        DQ[Data Quality Service<br/>(ETL - Python)]
        ENR[("Kafka Topic:<br/>events-enriched")]
    end

    subgraph "Analytics Layer"
        BATCH[Spark Batch Job<br/>(Daily Aggregation)]
        STREAM[Spark Streaming Job<br/>(Real-Time KPIs)]
    end

    subgraph "Output"
        REP1[Console Report:<br/>Daily Active Users]
        REP2[Live Dashboard:<br/>Revenue & Users / Min]
    end

    %% Flow Connections
    GEN -->|1. JSON Events| RAW
    
    RAW -->|2a. Read History| BATCH
    BATCH --> REP1

    RAW -->|2b. Consume| DQ
    DQ -->|3. Transform & Enrich| ENR
    
    ENR -->|4. Read Stream| STREAM
    STREAM --> REP2

    %% Styling
    style GEN fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style RAW fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style ENR fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style DQ fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style BATCH fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style STREAM fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```

### Data Flow Explained

1.  **Ingestion**: The `data_generator.py` simulates game clients sending raw events (`Init`, `Match`, `InAppPurchase`) to the **Kafka** topic `events-raw`.
2.  **Data Quality (ETL)**: The `data_quality.py` service consumes raw events, normalizes data (e.g., standardizing Country Names and Platform casing), and produces clean data to the `events-enriched` topic.
3.  **Batch Processing**: `batch_processing.py` reads historical raw data to calculate high-precision daily metrics (Daily Active Users).
4.  **Real-Time Processing**: `stream_aggregation.py` reads the enriched stream to calculate financial and engagement metrics with 1-minute latency.

---

## 📂 Project Structure

- **`src/`**
  - **`data_generator.py`**: (Phase I) Simulates game events and publishes them to Kafka topic `events-raw`.
  - **`batch_processing.py`**: (Phase I) Spark batch job that computes daily unique users from historical data.
  - **`data_quality.py`**: (Phase II) **Real-Time Service**. Consumes `events-raw`, uppercases platforms, maps country codes to names, and produces to `events-enriched`.
  - **`stream_aggregation.py`**: (Phase III) **Real-Time Spark Job**. Consumes `events-enriched` and calculates revenue, purchases, and user counts per minute.
- **`schemas/`**: JSON schemas for the events.
- **`docker-compose.yml`**: Infrastructure (Zookeeper, Kafka, Spark Master/Worker, App Container).
- **`Dockerfile`**: Environment definition for the Python/Spark application.

---

## 🚀 How to Run the Full Pipeline

### 1. Start the Environment
Build and start all services. The data generator starts automatically.

```bash
docker-compose up --build -d
```

### 2. Run Phase I: Batch Processing (Daily Analytics)
Calculate historical daily unique users.

```bash
docker exec miniclip-app python src/batch_processing.py
```

### 3. Run Phase II: Real-Time Data Quality Service
Start the service that cleans and enriches data in the background.

```bash
docker exec -d miniclip-app python src/data_quality.py
```
*Note: `-d` runs it in detached mode (background).*

### 4. Run Phase III: Real-Time Aggregation (Spark Streaming)
Start the Spark Streaming job to see real-time metrics on your console.

```bash
docker exec miniclip-app python src/stream_aggregation.py
```

---

## 📊 Execution Results (Demo)

### 1. Phase I Output (Daily Batch)
*Example output showing distinct users aggregated by day, country, and platform.*

```text
+----------+-------+--------+--------------+
|date      |country|platform|distinct_users|
+----------+-------+--------+--------------+
|2025-12-10|BR     |android |1             |
|2025-12-10|BR     |ios     |2             |
|2025-12-10|DE     |facebook|2             |
|2025-12-10|US     |ios     |3             |
+----------+-------+--------+--------------+
```

### 2. Phase III Output (Real-Time Streaming)
*Streaming batches updating every minute.*

**A. Revenue by Country (Enriched Data)**
*Notice the Country Names are now full names (e.g. "United States" instead of "US"), proving the Data Quality service works.*

```text
+------------------------------------------+--------------+------------------+
|window                                    |country       |country_revenue   |
+------------------------------------------+--------------+------------------+
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|United Kingdom|5090.009999999999|
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|Portugal      |5827.689999999999|
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|United States |6632.530000000001|
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|Germany       |6036.629999999999|
+------------------------------------------+--------------+------------------+
```

**B. Financial KPIs (1-min Window)**

```text
+------------------------------------------+---------------+-----------------+
|window                                    |total_purchases|total_revenue    |
+------------------------------------------+---------------+-----------------+
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|772            |77826.37999999995|
+------------------------------------------+---------------+-----------------+
```

**C. Distinct Users (1-min Window)**

```text
+------------------------------------------+--------------+
|window                                    |distinct_users|
+------------------------------------------+--------------+
|{2025-12-10 01:20:00, 2025-12-10 01:21:00}|1133          |
+------------------------------------------+--------------+
```

---

## 🛠 Tech Stack
- **Language**: Python 3.9
- **Streaming**: Kafka, Spark Structured Streaming
- **Processing**: Apache Spark 3.5.0
- **Containerization**: Docker, Docker Compose
