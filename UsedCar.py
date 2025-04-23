import sqlite3
import requests
from datetime import date, timedelta

DB_NAME = "final_project.db"
CITIES = [
    {"city": "Austin", "state": "Texas"},
    {"city": "Chicago", "state": "Illinois"}
]

# Total days of data you want
TOTAL_DAYS = 100
DAYS_PER_CALL = 25

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

def get_lat_lon(city_name):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1&language=en&format=json"
    response = requests.get(url)
    response.raise_for_status()
    geo = response.json()
    result = geo["results"][0]
    return result["latitude"], result["longitude"]

def fetch_weather(lat, lon, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
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
        temp = data["daily"]["temperature_2m_max"][i]
        rain = data["daily"]["precipitation_sum"][i]
        wind = data["daily"]["windspeed_10m_max"][i]

        cur.execute('''
            INSERT INTO WeatherConditions (state, city, date, temperature, precipitation, windspeed)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (state, city, date_val, temp, rain, wind))

    conn.commit()
    conn.close()

def main():
    create_table()
    today = date.today() - timedelta(days=1)

    for entry in CITIES:
        city = entry["city"]
        state = entry["state"]
        print(f"\n🌤 Fetching 100-day weather history for {city}, {state}...")
        
        try:
            lat, lon = get_lat_lon(city)

            # Fetch 100 days in chunks of 25 days
            for i in range(0, TOTAL_DAYS, DAYS_PER_CALL):
                end_date = today - timedelta(days=i)
                start_date = end_date - timedelta(days=DAYS_PER_CALL - 1)
                print(f"   📅 Requesting: {start_date} to {end_date}")
                
                data = fetch_weather(lat, lon, start_date, end_date)
                insert_weather(city, state, data)

            print(f"✅ Done inserting 100 weather entries for {city}")
        except Exception as e:
            print(f"❌ Failed for {city}, {state}: {e}")

if __name__ == "__main__":
    main()









