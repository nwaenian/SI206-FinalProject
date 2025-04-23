import requests
import sqlite3
import time

DB_NAME = 'crash_weather_project.db'
LIMIT = 25

# City configurations
CITIES = {
    'Chicago': {
        'url': 'https://data.cityofchicago.org/resource/85ca-t3if.json',
        'id_field': 'crash_record_id',
    },
    'Austin': {
        'url': 'https://data.austintexas.gov/resource/y2wy-tgr5.json',
        'id_field': 'cris_crash_id',
    }
}

def create_table():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS crashes (
            city TEXT,
            crash_id TEXT PRIMARY KEY,
            timestamp TEXT,
            latitude REAL,
            longitude REAL,
            injuries_total INTEGER,
            fatalities INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def get_existing_ids():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT crash_id FROM crashes")
    rows = cur.fetchall()
    conn.close()
    return set(row[0] for row in rows)

def store_crashes(city, data, id_field):
    existing_ids = get_existing_ids()
    new_entries = 0
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    for crash in data:
        crash_id = crash.get(id_field)
        if not crash_id or crash_id in existing_ids:
            continue
        try:
            cur.execute('''
                INSERT INTO crashes (city, crash_id, timestamp, latitude, longitude, injuries_total, fatalities)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                city,
                crash_id,
                crash.get('crash_date') or crash.get('crash_timestamp'),
                float(crash.get('latitude', 0)),
                float(crash.get('longitude', 0)),
                int(crash.get('injuries_total') or crash.get('tot_injry_cnt') or 0),
                int(crash.get('injuries_fatal') or crash.get('death_cnt') or 0)
            ))
            new_entries += 1
        except Exception as e:
            print(f"⚠️ Skipped record: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"✅ {new_entries} new records from {city} inserted.")

def fetch_and_store(city, config):
    print(f"\n🌆 Collecting crash data from {city}")
    limit = 25
    offset = 0
    total_fetched = 0
    max_records = 100

    while total_fetched < max_records:
        params = {
            '$limit': LIMIT,
            '$offset': offset
        }
        try:
            response = requests.get(config['url'], params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data:
                print(f"⚠️ No data returned for {city} at offset {offset}")
                break

            print(f"🔍 Preview of data from offset {offset}:")
            for i, entry in enumerate(data[:3]):
                print(f"Record {i + 1}: {entry}")
            store_crashes(city, data, config['id_field'])

            total_fetched += limit
            offset += limit
            time.sleep(1)

        except Exception as e:
            print(f"❌ Failed to fetch data for {city} at offset {offset}: {e}")
            break

if __name__ == "__main__":
    create_table()
    for city, config in CITIES.items():
        fetch_and_store(city, config)