import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
SAVE_DIR = "../data/raw"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
FILES = [f"yellow_tripdata_2019-{month}.parquet" for month in MONTHS]

# Created a directory to save the files
os.makedirs(SAVE_DIR, exist_ok=True)


# Set up a requests session with retry strategy 5 retries.
def setup_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


#used try and except to download files

def download_file(session, url, save_path):
    try:
        response = session.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        print(f"Downloaded: {save_path}")
    except requests.exceptions.RequestException as err:
        print(f"Error downloading {url}: {err}")


def main():

    session = setup_session()

    for file_name in FILES:
        file_url = f"{BASE_URL}{file_name}"
        save_path = os.path.join(SAVE_DIR, file_name)
        download_file(session, file_url, save_path)


if __name__ == "__main__":
    main()
