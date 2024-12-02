"""

from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve, URLError
import os
from pathlib import Path
import re
from urllib.parse import urljoin

# Base directory of the repository
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd() 
    
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

    # Grab all links that might contain downloadable files
    links = []
    for link in soup.find_all('a', href=True):
        file_type = link['href'].split('.')[-1]
        if file_type in ["pdf", "xls", "xlsx", "csv"]:  # limited to just these file types for now
            full_url = urljoin(dataurl, link['href'])
            filename_from_url = full_url.rsplit('/', 1)[-1]
            filename_from_text = link.get_text(strip=True).replace(' ', '_').replace('/', '-').replace(',', '')
            links.append((full_url, filename_from_url, filename_from_text, file_type))

    # Print out all downloadable files with IDs and both filename options
    for i, (url, filename_from_url, filename_from_text, file_type) in enumerate(links, 1):
        print(f"{i}. URL Filename: {filename_from_url} ({file_type}); Text Filename: {filename_from_text}; File type: {file_type}; URL link: {url}\n")

    # User input to select files
    print("*" * 30)
    selected_files = input("Which files do you want to download? Enter IDs separated by commas (e.g., 1,3,5), by ranges (e.g., 4-7), or type 'all' for all files: ")

    # Determine which IDs to download based on user input
    selected_ids = []
    if selected_files.lower() == 'all':
        selected_ids = list(range(1, len(links) + 1))
    else:
        for part in selected_files.split(','):
            if '-' in part:
                start, end = map(int, part.split('-'))
                selected_ids.extend(range(start, end + 1))
            else:
                selected_ids.append(int(part))

    # Loop over selected file IDs and download them
    for id in selected_ids:
        url, filename_from_url, filename_from_text, file_type = links[id - 1]
        filename = os.path.join(RAW_DATA_DIR, filename_from_url)  # Choose here if you prefer filename_from_url or filename_from_text

        # Check if the file already exists
        if os.path.isfile(filename):
            print(f"File {filename_from_url} already exists, skipping download.\n")
            continue

        try:
            # Download the file
            print(f"Downloading {filename_from_url} to {filename}\n")
            urlretrieve(url, filename)
        except Exception as e:
            print(f"An error occurred while downloading {filename_from_url}: {e}\n")

except URLError as e:
    print(f"An error occurred: {e}. This may be due to a network issue or an incorrect URL.\n")
except Exception as e:
    print(f"An unexpected error occurred: {e}\n")
    
    
"""
    
    
#%%

import os
import time
from urllib.parse import urljoin
from urllib.request import urlopen, Request, URLError
from bs4 import BeautifulSoup
import requests
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options



### FUNCTIONS
def setup_chrome_driver(download_dir):
    """It is necessary to have a Chromedriver for the webarchived files, that use Javascript to be downloaded in the browser"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True
    })
    driver_path = ChromeDriverManager().install()
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)


def download_file(url, filename):
    """ Download file using Selenium for complex JavaScript-based redirection or direct download for simpler cases """
    try:
        if 'webarchive' or 'web.archive' in url:
            print('webarchive URL')
            driver = setup_chrome_driver(os.path.dirname(filename))
            driver.get(url)
            time.sleep(10)  # Adjust time for file to download
            driver.quit()
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            print('normal / other URL')
            response = requests.get(url, headers=headers, allow_redirects=True)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False


### DEFINING DIRECTORIES
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()  # This is if you work in interactive shells (e.g., IPython, Jupyter notebooks)

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "supporting-facilities")
DERIVED_DATA_DIR = os.getenv("DERIVED_DATA_DIR", BASE_DIR / "data")
os.makedirs(RAW_DATA_DIR, exist_ok=True)



### DOWNLOADING DATA
dataurl = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancelled-elective-operations/supporting-facilities-data/"
try:
    page = urlopen(Request(dataurl, headers={'User-Agent': 'Mozilla/5.0'}))
    soup = BeautifulSoup(page, features="html.parser")
    
    links = []
    for link in soup.find_all('a', href=True):
        file_type = link['href'].split('.')[-1].lower()
        if file_type in ["pdf", "xls", "xlsx", "csv"]:
            full_url = urljoin(dataurl, link['href'])
            filename_from_url = full_url.rsplit('/', 1)[-1]
            filename_from_text = link.get_text(strip=True).replace(' ', '_').replace('/', '-').replace(',', '')
            links.append((full_url, filename_from_url, filename_from_text, file_type))

    for i, (url, filename_from_url, filename_from_text, file_type) in enumerate(links, 1):
        print(f"{i}. URL Filename: {filename_from_url}; Text Filename: {filename_from_text}; URL: {url}\n")

    selected_files = input("Which files to download? Enter IDs (e.g., 1,3,5), ranges (e.g., 4-7), or 'all': ")

    selected_ids = []
    if selected_files.lower() == 'all':
        selected_ids = list(range(1, len(links) + 1))
    else:
        for part in selected_files.split(','):
            if '-' in part:
                start, end = map(int, part.split('-'))
                selected_ids.extend(range(start, end + 1))
            else:
                selected_ids.append(int(part))

    for id in selected_ids:
        url, filename_from_url, filename_from_text, file_type = links[id - 1]
        filename = os.path.join(RAW_DATA_DIR, filename_from_url)

        if os.path.isfile(filename):
            print(f"File {filename_from_url} already exists, skipping.\n")
            continue

        print(f"Downloading {filename_from_url}...\n")
        success = download_file(url, filename)
        if success:
            print(f"Successfully downloaded {filename_from_url}\n")
        else:
            print(f"Failed to download {filename_from_url}\n")
        time.sleep(1)  # Polite delay between downloads

except URLError as e:
    print(f"Network error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
    
    
