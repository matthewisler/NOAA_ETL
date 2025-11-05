# New York Weather ETL Pipeline (1974–2024)

This project is a Python-based ETL (Extract, Transform, Load) pipeline that retrieves 50 years of historical weather data for New York State from the NOAA Climate Data Online API. It demonstrates a complete data engineering workflow including API integration, data cleaning, transformation, analytics, and database loading.

---

## Overview

The pipeline performs three main stages:

1. **Extract**  
   Retrieves daily weather data (maximum temperature, minimum temperature, and precipitation) from NOAA’s API for all New York weather stations.  
   Includes pagination, retry logic, and monthly batching to handle API limits.

2. **Transform**  
   Cleans, reshapes, and aggregates the data into structured form:
   - Calculates annual average temperature (°C)
   - Calculates total annual precipitation (mm)
   - Generates per-station summaries
   - Computes long-term temperature trends using linear regression

3. **Load**  
   Saves the processed results as:
   - CSV files
   - SQLite database tables
   - Visual trend plots for temperature and precipitation

---

## Tech Stack

| Category | Tools / Libraries |
|-----------|------------------|
| Language | Python 3.10+ |
| Data Processing | pandas, numpy |
| Visualization | matplotlib |
| API Requests | requests |
| Statistics | scipy (linregress) |
| Storage | SQLite, CSV |
| Logging / Config | logging, python-dotenv |

---

## Features

- Reliable API ingestion with retry and exponential backoff  
- Monthly batching to avoid timeouts and API rate limits  
- Auto-resume in case of failure during extraction  
- Clean data transformation and aggregation logic  
- Regression analysis for long-term trends  
- CSV and SQLite outputs for flexibility  
- Structured logging and modular design  

---

## Project Structure
```yaml
etl_noaa/
├── etl_noaa.py # Main ETL script
├── average_temp_ny_graph.png
├── code_log.txt
├── new_york_weather_top_1000.csv # File too large, here is the first 1000 rows
├── ny_annual_climate_summary.csv
├── ny_station_summary.csv
├── ny_weather.db
├── total_percipitation_ny.png
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/<yourusername>/etl-noaa.git
cd etl-noaa
```
### 2. Create a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows
```
### 3. Install Dependencies
```bash
pip install -r requirements.txt
```
### 4. Configure Environment Variables
Create a .env file in the project root and add your NOAA API token:

```ini

NOAA_TOKEN=your_noaa_api_key_here
You can request a free NOAA API token at:
https://www.ncdc.noaa.gov/cdo-web/token
```

### 5. Run the Pipeline
```bash
python src/etl_noaa.py
```
### Outputs
After running the pipeline, the following files are generated:

### File	Description
data/new_york_weather_1974_2024.csv	-> Combined raw dataset

data/ny_annual_climate_summary.csv ->	Yearly averages and precipitation totals

data/ny_station_summary.csv	-> Station-level summary data

data/avg_temp_trend.png	-> Annual average temperature trend

data/total_precipitation_trend.png	-> Annual total precipitation trend

data/ny_weather.db	-> SQLite database containing the weather data

### Example Query
Example of querying the SQLite database for data since the year 2000:

```python

import sqlite3
import pandas as pd

conn = sqlite3.connect("data/ny_weather.db")
query = "SELECT year, avg_temp, total_precip FROM Weather_data WHERE year >= 2000;"
df = pd.read_sql(query, conn)
conn.close()

print(df.head())
```
### Example Visualization
A sample output plot shows how the average annual temperature in New York has changed from 1974 to 2024, with an upward trend of roughly 0.25 °C per decade.

### Key Learnings
-Building a resilient ETL process for real-world APIs

-Managing API pagination, retries, and error handling

-Performing time-series aggregation and trend analysis

-Loading and querying data using SQLite

-Generating reproducible visualizations and outputs

-Writing modular, production-style Python code

### Future Improvements
-Add workflow orchestration using Prefect or Airflow

-Build a Streamlit dashboard for interactive analysis

-Extend to additional states or data types

-Automate periodic data refreshes

