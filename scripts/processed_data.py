import os
import pandas as pd

RAW_DATA_DIR = "../data/converted_to_csv"
PROCESSED_DATA_DIR = "../data/processed"
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)


def remove_empty_columns(df):

    return df.dropna(axis=1, how='all')


def process_file(file_path):

    # Define data types for the columns
    dtype = {
        'VendorID': 'int64',
        'passenger_count': 'float64',
        'trip_distance': 'float64',
        'RatecodeID': 'float64',
        'store_and_fwd_flag': 'str',
        'PULocationID': 'int64',
        'DOLocationID': 'int64',
        'payment_type': 'int64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'congestion_surcharge': 'float64',
        'airport_fee': 'float64'
    }

    # Read the CSV file with specified data types
    df = pd.read_csv(file_path, dtype=dtype, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                     low_memory=False)
    print(f"Initial data loaded: {df.shape[0]} rows")

    # Remove columns that contain only empty values
    df = remove_empty_columns(df)
    print(f"After removing empty columns: {df.shape[1]} columns")

    # Remove rows with missing or corrupt data
    df = df.dropna()
    print(f"After dropping missing values: {df.shape[0]} rows")

    # Derive new columns
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    print(f"After deriving new columns: {df.shape[0]} rows")

    # Remove rows with invalid values
    df = df[(df['trip_duration'] > 0) & (df['average_speed'] > 0)]
    print(f"After filtering invalid values: {df.shape[0]} rows")

    return df

#Aggregated data to calculate total trips and average fare per day.
def aggregate_data(df):

    df['date'] = df['tpep_pickup_datetime'].dt.date
    daily_stats = df.groupby('date').agg(
        total_trips=('fare_amount', 'size'),
        average_fare=('fare_amount', 'mean')
    ).reset_index()

    return daily_stats


def save_data(df, file_basename):
    """Save the joined data."""
    # Aggregated data
    daily_stats = aggregate_data(df)


    joined_df = pd.merge(df, daily_stats, left_on=df['tpep_pickup_datetime'].dt.date, right_on='date',
                         suffixes=('', '_daily'))
    joined_df.drop(columns='date', inplace=True)  # Remove the duplicate date column

    joined_file_path = os.path.join(PROCESSED_DATA_DIR, f'{file_basename}.csv')
    joined_df.to_csv(joined_file_path, index=False)
    print(f"Joined data saved: {joined_file_path}")


def main():

    for file_name in os.listdir(RAW_DATA_DIR):
        if file_name.endswith('.csv'):
            file_path = os.path.join(RAW_DATA_DIR, file_name)
            file_basename = os.path.basename(file_name).split('.')[0]
            df = process_file(file_path)
            if df.shape[0] > 0:
                save_data(df, file_basename)
            else:
                print(f"No valid data in file: {file_path}")


if __name__ == "__main__":
    main()