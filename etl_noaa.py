# Code for ETL operations on New York weather data

# Importing the required libraries
import requests
import os
import pandas as pd
import sqlite3
from datetime import datetime
from time import sleep
from calendar import monthrange
from scipy.stats import linregress
import matplotlib.pyplot as plt

NOAA_TOKEN = os.getenv('NOAA_TOKEN')
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": NOAA_TOKEN}
output_csv_name = "new_york_weather_1974_2024.csv"

BASE_DIR = os.path.dirname(__file__)
FILE_PATH = os.path.join(BASE_DIR, "data", "new_york_weather_1974_2024.csv")

TABLE_NAME = 'Weather_data'
DB_NAME = "ny_weather.db"

def log_progress(message: str) -> None:
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    # Year-Monthname-Day-Hour-Minute-Second
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    # get current timestamp 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def load_to_csv(df: pd.DataFrame, output_path:str) -> None:
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.Connection, TABLE_NAME: str) -> None:
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(TABLE_NAME, sql_connection, if_exists = 'replace', index = False)

def run_queries(query_statement: str, sql_connection: sqlite3.Connection) -> None:
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def make_request(headers, params, retries=5, backoff=2):
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            if response.status_code == 200:
                return response
            elif response.status_code == 503:
                wait = backoff ** attempt
                
                sleep(wait)
            elif response.status_code == 429:
                print(f"Error with status code {response.status_code}: {response.text}")
                return None 
            else:
                print(f"Error with status code {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            sleep(backoff ** attempt)
    print("Max retries exceeded.")
    return None

def get_temp_vals_by_dates(start: str, end: str) -> pd.DataFrame:
    params = {
        "datasetid": "GHCND",
        "locationid": "FIPS:36",
        "startdate": start,
        "enddate": end,
        "datatypeid": ["TMAX", "TMIN", "PRCP"],
        "units": "metric",
        "limit": 1000,
        "offset": 1,
    }

    results = []
    has_more = True

    while has_more:
        response = make_request(headers=headers, params=params)
            
        if response.status_code != 200:
            print(f"Failed to retrieve request for startdate-enddate {start}-{end}: with status code {response.status_code}")
            df = pd.DataFrame(results)
            return df

        print("had a valid request")
        data = response.json()
        batch = data.get("results", [])
        results.extend(batch)

        meta = data.get("metadata", {}).get("resultset", {})
        offset = meta.get("offset", 0)
        limit = meta.get("limit", 0)
        total = meta.get("count", 0)

        print(f"  - {min(offset + len(batch), total)} / {total} records")


        if offset + limit >=total:
            has_more = False
        else:
            params["offset"] += limit
            sleep(0.2)

    df = pd.DataFrame(results)
    print(df.head())
    return df

def generate_date_ranges(year: int) -> list[str]:
    months = []
    for month in range(1, 13):
        days = monthrange(year, month)[1]
        months.append((f"{year}-{month:02d}-01", f"{year}-{month:02d}-{days:02d}"))
    return months

def fetch_year(year: int) -> pd.DataFrame:
    months = generate_date_ranges(year)
    dfs = []

    for start, end in months:
        df_month = get_temp_vals_by_dates(start, end)
        if not df_month.empty:
            df_month["year"] = year
            dfs.append(df_month)
        sleep(0.5)

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        print(f"No data return for {year}")
        return pd.DataFrame()


def extract(start_year: int, end_year: int) -> pd.DataFrame:
    all_years = []    

    for year in range(start_year, end_year):
        df_year = fetch_year(year)
        if not df_year.empty:
            all_years.append(df_year)

    if all_years:
        df_all = pd.concat(all_years, ignore_index=True)
        df_all.to_csv(output_csv_name, index=False)
        print(f"Saved records!")
        return df_all

    else:
        print("Failed to save data")

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["date", "datatype", "station", "value", "year"]].copy()

    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])

    # NOAA gives tenths of °C or mm; adjust accordingly
    # (Some stations report already scaled, so check typical range)
    df["value"] = df["value"].astype(float)

    # Separate datatypes into columns
    df_pivot = df.pivot_table(
        index=["date", "station", "year"],
        columns="datatype",
        values="value",
        aggfunc="first"
    ).reset_index()

    annual_summary = df_pivot.groupby("year").agg(
        avg_tmax=("TMAX", "mean"),
        avg_tmin=("TMIN", "mean"),
        avg_temp=("TMAX", lambda x: (x.mean() + df_pivot.loc[x.index, "TMIN"].mean()) / 2),
        total_precip=("PRCP", "sum"),
        station_count=("station", "nunique")
    ).reset_index()

    print(annual_summary.head())

    slope, intercept, r_value, p_value, std_err = linregress(
        annual_summary["year"], annual_summary["avg_temp"]
    )
    print(f"Trend: {slope:.3f} °C per year ({slope*10:.2f} °C per decade)")
    if not os.path.isfile("C:\\Users\MattI\\etl_noaa\\average_temp_ny_graph.png"):
        plt.figure(figsize=(10,5))
        plt.plot(annual_summary["year"], annual_summary["avg_temp"], label="Avg Temp (°C)")
        plt.title("Average Annual Temperature in New York (1974–2024)")
        plt.xlabel("Year")
        plt.ylabel("Temperature (°C)")
        plt.grid(True)
        plt.legend()
        plt.show()

    if not os.path.isfile("C:\\Users\\MattI\\etl_noaa\\total_percipitation_ny.png"):
        plt.figure(figsize=(10,5))
        plt.bar(annual_summary["year"], annual_summary["total_precip"])
        plt.title("Total Annual Precipitation in New York (mm)")
        plt.xlabel("Year")
        plt.ylabel("Total Precipitation (mm)")
        plt.show()
    

    station_summary = df_pivot.groupby("station").agg(
        mean_tmax=("TMAX", "mean"),
        mean_tmin=("TMIN", "mean"),
        mean_precip=("PRCP", "mean"),
        data_years=("year", "nunique")
    ).reset_index()

    top5_hot = station_summary.sort_values("mean_tmax", ascending=False).head()
    print(f"top5_hot: {top5_hot}")

    top5_wet = station_summary.sort_values("mean_precip", ascending=False).head()
    print(f"top5_wet: {top5_wet}")

    annual_summary.to_csv("ny_annual_climate_summary.csv", index=False)
    station_summary.to_csv("ny_station_summary.csv", index=False)   

    return df



log_progress('Preliminaries complete. Initiating ETL process.')
if not os.path.isfile(FILE_PATH):
    log_progress('No CSV available...extracting the data')
    df = extract(1974, 2024)
else:
    log_progress('CSV available...utilizing previous CSV')
    df = pd.read_csv(FILE_PATH)


log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process.')

log_progress('Data saved to CSV file.')


sql_connection = sqlite3.connect(DB_NAME)
print(sql_connection)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, TABLE_NAME)
log_progress("Data loaded to Database as table. Running the query.")

query_statement = f"SELECT date, station, MAX(value) AS max_temp, year FROM {TABLE_NAME} WHERE datatype = 'TMAX' GROUP BY station ORDER BY station"
run_queries(query_statement, sql_connection)

log_progress("Process Complete.")

sql_connection.close()
