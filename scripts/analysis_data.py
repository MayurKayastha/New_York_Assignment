import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_PATH = "../New_York_Taxi.db"
VISUALIZATIONS_DIR = "../BI_visuals"
os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)


def query_database(query):

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def peak_hours():

    query = '''
        SELECT strftime('%H', tpep_pickup_datetime) AS hour, COUNT(*) AS trip_count
        FROM yellow_tripdata
        GROUP BY hour
        ORDER BY hour
    '''
    df = query_database(query)
    print(df)

    if df.empty:
        print("No data available for peak hours.")
        return

    plt.figure(figsize=(10, 6))
    sns.barplot(x='hour', y='trip_count', data=df)
    plt.title('Peak Hours for Taxi Usage')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Trips')
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'peak_hours.png'))
    plt.show()


def passenger_count_vs_fare():

    query = '''
        SELECT passenger_count, AVG(total_amount) AS avg_fare
        FROM yellow_tripdata
        GROUP BY passenger_count
        ORDER BY passenger_count
    '''
    df = query_database(query)
    print(df)

    if df.empty:
        print("No data available for passenger count vs fare.")
        return

    plt.figure(figsize=(10, 6))
    sns.barplot(x='passenger_count', y='avg_fare', data=df)
    plt.title('Passenger Count vs. Average Fare')
    plt.xlabel('Passenger Count')
    plt.ylabel('Average Fare ($)')
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'passenger_count_vs_fare.png'))
    plt.show()


def usage_trends():

    query = '''
        SELECT strftime('%Y-%m', tpep_pickup_datetime) AS month, COUNT(*) AS trip_count
        FROM yellow_tripdata
        GROUP BY month
        ORDER BY month
    '''
    df = query_database(query)
    print(df)

    if df.empty:
        print("No data available for usage trends.")
        return

    plt.figure(figsize=(10, 6))
    sns.lineplot(x='month', y='trip_count', data=df)
    plt.title('Trends in Taxi Usage Over the Year')
    plt.xlabel('Month')
    plt.ylabel('Number of Trips')
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'usage_trends.png'))
    plt.show()


def check_table():
    query = "SELECT * FROM yellow_tripdata LIMIT 5"
    df = query_database(query)
    print("Sample Data from yellow_tripdata table:")
    print(df)


def check_columns():
    query = "PRAGMA table_info(yellow_tripdata)"
    df = query_database(query)
    print("Columns and Data Types in yellow_tripdata table:")
    print(df)


def main():

    check_table()
    check_columns()
    peak_hours()
    passenger_count_vs_fare()
    usage_trends()


if __name__ == "__main__":
    main()
