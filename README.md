# Miniclip Data Engineering Task

This project implements a real-time data processing pipeline for gaming events (`Init`, `Match`, `InAppPurchase`) as per the Miniclip Data Engineering Task requirements.

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| **Phase I** | **Kafka Setup & Spark Batch Aggregation** <br> - Dockerized environment (Kafka, Zookeeper, Spark). <br> - Python Data Generator sending events to Kafka. <br> - Spark Batch Job to calculate daily unique users by country/platform. | ✅ Completed |
| **Phase II** | **Real-Time Data Quality (Kafka Streams)** <br> - Stream processing to clean and enrich data (uppercase, mapping). | ⏳ Pending |
| **Phase III** | **Spark Streaming Aggregation** <br> - Real-time minute-level aggregation (revenue, users, matches). | ⏳ Pending |

---

## 🚀 How to Run Phase I

### 1. Start the Environment
Build and start all services (Kafka, Zookeeper, Spark, and the Python App):

```bash
docker-compose up --build -d
```

Once running, the `miniclip-app` container automatically starts generating synthetic events to the `events-raw` Kafka topic.

### 2. Verify Data Generation
Check the logs to ensure events are being produced:

```bash
docker logs -f miniclip-app
```
*(Press `Ctrl+C` to exit logs)*

### 3. Run Spark Batch Job (Phase I Task)
Execute the Spark batch script to calculate the **Daily Aggregated Data** (Distinct users by Country and Platform):

```bash
docker exec miniclip-app python src/batch_processing.py
```

**Expected Output:**
A console table showing the aggregation results:

```text
+----------+-------+--------+--------------+
|date      |country|platform|distinct_users|
+----------+-------+--------+--------------+
|2023-12-06|US     |ios     |42            |
|2023-12-06|BR     |android |35            |
...
```

---

## 📂 Project Structure

- **`src/data_generator.py`**: Simulates game events and publishes them to Kafka topic `events-raw`.
- **`src/batch_processing.py`**: Spark batch job that reads from Kafka, processes `init` events, and computes daily unique users.
- **`docker-compose.yml`**: Orchestrates Zookeeper, Kafka, Spark Master/Worker, and the App container.
- **`Dockerfile`**: Custom image for the Python app, including Java (for Spark) and required Python libraries.

