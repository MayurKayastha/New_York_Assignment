import os
import pandas as pd

RAW_DATA_DIR = "../data/raw"
PROCESSED_DATA_DIR = "../data/converted_to_csv"
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

#Try catch to convert parquet to csv
def convert_to_csv(file_path):

    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        csv_file_path = os.path.join(PROCESSED_DATA_DIR, os.path.basename(file_path).replace('.parquet', '.csv'))
        df.to_csv(csv_file_path, index=False)
        print(f"Converted: {csv_file_path}")
    except ImportError as e:
        print(f"Error: {e}")
        print("Please install 'pyarrow' using pip: pip install pyarrow")


def main():

    for file_name in os.listdir(RAW_DATA_DIR):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(RAW_DATA_DIR, file_name)
            convert_to_csv(file_path)


if __name__ == "__main__":
    main()
