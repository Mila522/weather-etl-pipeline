# 🌦️ Live Weather ETL Pipeline

A Python-based ETL pipeline that fetches real-time weather data from the **Open-Meteo API**, transforms it using **Pandas**, and loads the results into **Microsoft SQL Server**.  
It automatically creates the database and table if they don’t exist — making the workflow fully operational from the first run.

---

## ✅ Features

- Real-time weather extraction for Pretoria (ZA)
- Automated database and table creation
- Clean transformation using Pandas DataFrame
- Logged ETL execution stages for monitoring & debugging
- Easily extendable for scheduled automation (e.g., Task Scheduler, Airflow)

---

## 🛠️ Tech Stack

| Component | Purpose |
|----------|---------|
| Python 3.9+ | Main runtime |
| Requests | API calls |
| Pandas | Data processing |
| SQLAlchemy + pyodbc | SQL Server integration |
| Open-Meteo API | Weather data source |

---

## 📦 Installation

Clone the repository:

```bash
git clone https://github.com/USERNAME/weather-etl-pipeline.git
cd weather-etl-pipeline
