import sqlite3
import requests
from datetime import date, timedelta

DB_NAME = "final_project.db"

# Cities you care about
CITIES = [
    {"city": "Detroit", "state": "Michigan"},
    {"city": "Miami", "state": "Florida"},
    {"city": "Los Angeles", "state": "California"},
    {"city": "Charleston", "state": "South Carolina"}
]

# 7-day range
END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=7)

# === STEP 1: Create the Weather Table ===
def create_table():
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

# === STEP 2: Get Lat/Lon from Geocoding API ===
def get_lat_lon(city_name):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1&language=en&format=json"
    response = requests.get(url)
    response.raise_for_status()
    geo = response.json()
    result = geo["results"][0]
    return result["latitude"], result["longitude"]

# === STEP 3: Get Weather Data ===
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

# === STEP 4: Store Data ===
def insert_weather(city, state, data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    for i, date_val in enumerate(data["daily"]["time"]):
        temp = data["daily"]["temperature_2m_max"][i]
        rain = data["daily"]["precipitation_sum"][i]
        wind = data["daily"]["windspeed_10m_max"][i]

        cur.execute('''
            INSERT INTO WeatherConditions (state, city, date, temperature, precipitation, windspeed)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (state, city, date_val, temp, rain, wind))

    conn.commit()
    conn.close()

# === MAIN ===
def main():
    create_table()
    for entry in CITIES:
        city = entry["city"]
        state = entry["state"]
        print(f"🌤 Fetching weather for {city}, {state}...")

        try:
            lat, lon = get_lat_lon(city)
            data = fetch_weather(lat, lon)
            insert_weather(city, state, data)
            print(f"✅ Inserted weather data for {city}")
        except Exception as e:
            print(f"❌ Failed for {city}, {state}: {e}")

if __name__ == "__main__":
    main()








