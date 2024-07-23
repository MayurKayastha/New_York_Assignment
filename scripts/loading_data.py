import os
import sqlite3
import pandas as pd

PROCESSED_DATA_DIR = "../data/processed"
DB_PATH = "../New_York_Taxi.db"


def create_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS taxi_trips (
            vendor_id TEXT,
            tpep_pickup_datetime TEXT,
            tpep_dropoff_datetime TEXT,
            passenger_count INTEGER,
            trip_distance REAL,
            rate_code_id INTEGER,
            store_and_fwd_flag TEXT,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount REAL,
            extra REAL,
            mta_tax REAL,
            tip_amount REAL,
            tolls_amount REAL,
            improvement_surcharge REAL,
            total_amount REAL,
            congestion_surcharge REAL,
            trip_duration REAL,
            average_speed REAL
        )
    ''')

    conn.commit()
    conn.close()


def load_data_to_db():
    conn = sqlite3.connect(DB_PATH)

    for file_name in os.listdir(PROCESSED_DATA_DIR):
        file_path = os.path.join(PROCESSED_DATA_DIR, file_name)
        df = pd.read_csv(file_path)
        df.to_sql('yellow_tripdata', conn, if_exists='append', index=False)
        print(f"Loaded to DB: {file_path}")

    conn.close()


def main():

    create_database()
    load_data_to_db()


if __name__ == "__main__":
    main()
