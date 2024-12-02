from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
import os
from pathlib import Path
import re
from urllib.parse import urljoin

# Base directory of the repository
BASE_DIR = Path(__file__).resolve().parent.parent

# Define paths
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "supporting-facilities")
DERIVED_DATA_DIR = os.getenv("DERIVED_DATA_DIR", BASE_DIR / "data")

# Ensure the raw data directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Data URL
dataurl = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancelled-elective-operations/supporting-facilities-data/"
try:
    # Fetch the web page
    page = urlopen(dataurl)
    soup = BeautifulSoup(page, features="html.parser")

    # Grab all .xls or .xlsx links from page
    links = []
    for link in soup.find_all('a', href=re.compile(r"\.xls[x]?$")):
        links.append(urljoin(dataurl, link['href']))

    # Loop over list and download files
    for row in links:
        # Extract file name from the URL
        name = row.rsplit('/', 1)[-1]

        # Combine file name with the raw data directory
        filename = os.path.join(RAW_DATA_DIR, name)

     # Check if the file already exists
        if os.path.isfile(filename):
            print(f"File {name} already exists, skipping download.")
            continue

        try:
            # Download the file
            print(f"Downloading {name} to {filename}")
            urlretrieve(row, filename)
        except Exception as e:
            print(f"An error occurred while downloading {name}: {e}")
            # Move on to the next file
            continue

except URLError as e:
    print(f"An error occurred: {e}. This may be due to a network issue or an incorrect URL.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")