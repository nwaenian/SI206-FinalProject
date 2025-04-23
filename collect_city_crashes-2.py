
import requests
import sqlite3
import os

DB_NAME = 'crash_weather_project.db'
LIMIT = 25
STATE_FILE = 'last_city.txt'

cities_info = {
    'Chicago': {
        'id': 1,
        'url': 'https://data.cityofchicago.org/resource/85ca-t3if.json?$limit=25&$order=crash_date DESC',
        'injuries_key': 'injuries_total',
        'fatalities_key': 'injuries_fatal',
        'vehicle_key': 'prim_contributory_cause',
        'weather_key': 'weather_condition',
        'crash_id_key': 'crash_record_id',
        'date_key': 'crash_date'
    },
    'Austin': {
        'id': 2,
        'url': 'https://data.austintexas.gov/resource/y2wy-tgr5.json?$limit=25&$order=crash_timestamp DESC',
        'injuries_key': 'tot_injry_cnt',
        'fatalities_key': 'death_cnt',
        'vehicle_key': 'units_involved',
        'weather_key': '',  # no direct weather field
        'crash_id_key': 'cris_crash_id',
        'date_key': 'crash_timestamp'
    }
}

# Alternate city logic
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as f:
        last_city = f.read().strip()
else:
    last_city = 'Austin'

next_city = 'Chicago' if last_city == 'Austin' else 'Austin'
with open(STATE_FILE, 'w') as f:
    f.write(next_city)

city = cities_info[next_city]
print(f"🌆 Pulling {LIMIT} crash records from {next_city}")

response = requests.get(city['url'])
data = response.json()

# Preview first few
print("\n🔍 Preview of data:")
if isinstance(data, list):
    for i, entry in enumerate(data[:3]):
        print(f"Record {i+1}: {entry}")
else:
    print("⚠️ Unexpected response format:", data)
    exit()

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

# Setup tables
cur.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS collisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        crash_id TEXT UNIQUE,
        crash_date TEXT,
        city_id INTEGER,
        injuries INTEGER,
        fatalities INTEGER,
        vehicle_type TEXT,
        weather TEXT,
        FOREIGN KEY (city_id) REFERENCES cities(id)
    )
''')

# Insert city info
cur.execute("INSERT OR IGNORE INTO cities (id, name) VALUES (?, ?)", (city['id'], next_city))
city_id = city['id']

inserted = 0
for crash in data:
    if not isinstance(crash, dict):
        continue
    try:
        crash_id = crash.get(city['crash_id_key'], '')
        crash_date = crash.get(city['date_key'], '')
        injuries = int(crash.get(city['injuries_key'], 0))
        fatalities = int(crash.get(city['fatalities_key'], 0))
        vehicle_type = crash.get(city['vehicle_key'], '')
        weather = crash.get(city['weather_key'], '') if city['weather_key'] else ''

        cur.execute('''
            INSERT OR IGNORE INTO collisions (crash_id, crash_date, city_id, injuries, fatalities, vehicle_type, weather)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            crash_id, crash_date, city_id, injuries, fatalities, vehicle_type, weather
        ))
        inserted += cur.rowcount
    except Exception as e:
        print("⚠️ Skipped record due to error:", e)

conn.commit()
conn.close()

print(f"\n✅ {inserted} new records from {next_city} inserted.")
