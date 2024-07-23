# Data Engineering Challenge: New York Taxi Data Processing

## Project Overview
This solution aims to design and implement a scalable data pipeline that extracts New York Taxi Trip data, processes it to derive analytical insights, and loads the processed data into a data warehouse for further analysis.

## Environment Setup
### Prerequisites
- Python 3.8+
- SQLite

### Installation
1. Clone the repository:
    ```sh
    git clone <repository_url>
    cd New_York_Assignment
    ```

2. Create and activate a virtual environment:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install the required packages:
    ```sh
    pip install -r prerequisites.txt
    ```

## Running the Project

### Data Extraction
To download the CSV files for the year 2019:
   ```sh
  python scripts/download_data.py
   ```

### Convert parquet to csv
To convert downloaded data to csv
   ```sh
python scripts/parquet_to_csv.py
   ```

### Data Processing
To clean and transform the downloaded data:
   ```sh
python scripts/processed_data.py
   ```

### Data Loading
To load the data into database:
   ```sh
python scripts/loading_data.py
   ```

### Data Analysis
To generate insights and visualizations:
   ```sh
   python scripts/analysis_data.py
   ```

