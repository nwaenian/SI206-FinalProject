import sqlite3
import pandas as pd
import requests
from datetime import date, timedelta

# === CONFIGURATION ===
DB_NAME = "final_project.db"
CRASH_DB = "crash_weather_project.db"  
CITIES = [
    {"city": "Austin", "state": "Texas"},
    {"city": "Chicago", "state": "Illinois"}
]

END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=25)

# === STEP 1: FETCH WEATHER DATA ===
def create_weather_table():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS WeatherConditions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT,
            city TEXT,
            date TEXT,
            temperature REAL,
            precipitation REAL,
            windspeed REAL
        )
    ''')
    conn.commit()
    conn.close()

def get_lat_lon(city_name):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1&language=en&format=json"
    response = requests.get(url)
    response.raise_for_status()
    result = response.json()["results"][0]
    return result["latitude"], result["longitude"]

def fetch_weather(lat, lon):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE.isoformat(),
        "end_date": END_DATE.isoformat(),
        "daily": "temperature_2m_max,precipitation_sum,windspeed_10m_max",
        "timezone": "America/New_York"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def insert_weather(city, state, data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for i, date_val in enumerate(data["daily"]["time"]):
        cur.execute('''
            INSERT INTO WeatherConditions (state, city, date, temperature, precipitation, windspeed)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            state.lower(), city.lower(), date_val,
            data["daily"]["temperature_2m_max"][i],
            data["daily"]["precipitation_sum"][i],
            data["daily"]["windspeed_10m_max"][i]
        ))
    conn.commit()
    conn.close()

# === STEP 2: IMPORT CRASH DATA ===
def import_crash_data():
    crash_conn = sqlite3.connect(CRASH_DB)
    crash_df = pd.read_sql_query("SELECT * FROM crashes", crash_conn)
    crash_conn.close()

    crash_df["date"] = pd.to_datetime(crash_df["timestamp"]).dt.date.astype(str)
    crash_df["city"] = crash_df["city"].str.lower()

    conn = sqlite3.connect(DB_NAME)
    crash_df.to_sql("Crashes", conn, index=False, if_exists="replace")
    conn.commit()
    conn.close()

# === STEP 3: COMBINE WEATHER + CRASH DATA ===
def combine_datasets():
    conn = sqlite3.connect(DB_NAME)
    weather_df = pd.read_sql_query("SELECT * FROM WeatherConditions", conn)
    crash_df = pd.read_sql_query("SELECT * FROM Crashes", conn)

    merged = pd.merge(crash_df, weather_df, how="inner", on="city", suffixes=("_crash", "_weather"))
    merged = merged[[
    "city", "date_crash", "state", "temperature", "precipitation", "windspeed", "injuries_total", "fatalities"
    ]]
    merged = merged.rename(columns={"date_crash": "date"})


    merged.to_sql("WeatherCrashCombined", conn, index=False, if_exists="replace")
    conn.commit()
    conn.close()

# === MAIN EXECUTION ===
if __name__ == "__main__":
    create_weather_table()
    for loc in CITIES:
        lat, lon = get_lat_lon(loc["city"])
        weather = fetch_weather(lat, lon)
        insert_weather(loc["city"], loc["state"], weather)
    import_crash_data()
    combine_datasets()
    print("✅ Combined weather and crash data saved to WeatherCrashCombined table.")





