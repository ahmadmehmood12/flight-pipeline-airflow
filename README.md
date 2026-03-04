# ✈️ Flights Medallion Data Pipeline with Apache Airflow

This project implements an **end-to-end Medallion Architecture data pipeline** using **Apache Airflow**, **PostgreSQL**, and **Snowflake**.
It ingests real-time flight data from the [OpenSky Network API](https://opensky-network.org/), processes it through **Bronze → Silver → Gold** layers, and loads aggregated metrics into **Snowflake** for analytics.

---

## 🧱 Architecture Overview

```
        ┌──────────────────────────┐
        │  OpenSky Network API     │
        └────────────┬─────────────┘
                     │
               (Bronze Ingestion)
                     │
        ┌────────────▼─────────────┐
        │   Raw JSON files (Bronze)│
        └────────────┬─────────────┘
                     │
             (Silver Transformation)
                     │
        ┌────────────▼─────────────┐
        │ Cleaned CSVs (Silver)    │
        └────────────┬─────────────┘
                     │
               (Gold Aggregation)
                     │
        ┌────────────▼─────────────┐
        │ Aggregated KPIs (Gold)   │
        └────────────┬─────────────┘
                     │
              (Load to Snowflake)
                     │
        ┌────────────▼─────────────┐
        │   Snowflake Table        │
        │   `FLIGHT_KPIS`          │
        └──────────────────────────┘
```

---

## ⚙️ Tech Stack

* **Apache Airflow** — Orchestrates the entire data pipeline
* **PostgreSQL** — Metadata backend for Airflow
* **Snowflake** — Data warehouse for storing gold metrics
* **Python**, **Pandas**, **Requests** — Data ingestion and transformation
* **OpenSky Network API** — Real-time public flight data source

---

## 🪶 DAG Structure

**DAG ID:** `flights_ops_medallion_pipe`
**Schedule:** Every 30 minutes (`*/30 * * * *`)

| Task ID                  | Description                                                                            |
| ------------------------ | -------------------------------------------------------------------------------------- |
| `bronze_ingest`          | Fetches raw flight data from OpenSky API and stores JSON in `/opt/airflow/data/bronze` |
| `silver_transform`       | Cleans and normalizes the Bronze data into a structured CSV                            |
| `gold_aggregate`         | Aggregates flight metrics by country (count, average velocity, on-ground flights)      |
| `load_gold_to_snowflake` | Merges Gold data into Snowflake table `FLIGHT_KPIS`                                    |

---

## 📂 Project Structure

```
├── dags/
│   └── flights_ops_medallion_pipe.py
├── scripts/
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_aggregate.py
│   └── load_gold_to_snowflake.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── README.md
```

---

## 🚀 Setup & Installation

### 1️⃣ Prerequisites

* Docker + Docker Compose (for Airflow setup)
* A Snowflake account
* Airflow connection configured with Snowflake credentials

### 2️⃣ Start Airflow

```bash
docker-compose up -d
```

### 3️⃣ Create Airflow Connection

Go to **Airflow UI → Admin → Connections → +**
Connection ID: `flight_snowflake`
Connection Type: `Snowflake`
Fill in your credentials:

| Field     | Example        |
| --------- | -------------- |
| Account   | `abc-xy12345`  |
| User      | `AIRFLOW_USER` |
| Password  | `********`     |
| Warehouse | `COMPUTE_WH`   |
| Database  | `FLIGHTS_DB`   |
| Schema    | `PUBLIC`       |
| Role      | `SYSADMIN`     |

---

## 🧮 Snowflake Table DDL

```sql
CREATE TABLE FLIGHT_KPIS (
    WINDOW_START TIMESTAMP,
    ORIGIN_COUNTRY STRING,
    TOTAL_FLIGHTS INT,
    AVG_VELOCITY FLOAT,
    ON_GROUND INT,
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 📈 Example Output (Gold Layer)

| origin_country | total_flights | avg_velocity | on_ground |
| -------------- | ------------- | ------------ | --------- |
| United States  | 152           | 240.8        | 45        |
| Germany        | 98            | 260.3        | 12        |
| France         | 67            | 242.9        | 8         |

---

## 🧰 Enhancements (Future Work)

* Add data validation and logging
* Store raw and intermediate data in Amazon S3
* Add unit tests for each transformation stage
* Create a dashboard in Tableau or Power BI using Snowflake data

---

## 👨‍💻 Author

Name : Ahmad Mehmood 
💼 Data Engineering Associate

📧 [ahmadmehmood1252@gmail.com](mailto:ahmadmehmood1252@gmail.com)
🌐 [https://www.linkedin.com/in/ahmadmehmood1252/](#)

---

## 🪪 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
