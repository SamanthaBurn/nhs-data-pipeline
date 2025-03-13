##########################################

# This python script will extract the links of datasets saved in .pdf, .xls, .xslx, .csv, from a given URL
# The script allows the selection of specific datasets in the URL and saved the selected files to folder specified in RAW_DATA_DIR

##########################################

### LIBRARIES

# pip install selenium
import os
import time
from urllib.parse import urljoin
from urllib.request import urlopen, Request, URLError
from bs4 import BeautifulSoup
import requests
from pathlib import Path
from extract_critical_care_beds_webarchive import handle_webarchive_download, setup_chrome_driver  # webarchive functionality


### FUNCTIONS

def get_file_extension(url, original_extension):
    """Extract the correct file extension from URL or original extension."""
    # First try to get extension from URL
    url_extension = url.split('.')[-1].lower() if '.' in url else ''
    # Clean and validate the extension
    valid_extensions = {'pdf', 'xls', 'xlsx', 'csv'}  # defined extensions for the file types we need
    if url_extension in valid_extensions:
        return f".{url_extension}"
    elif original_extension.lower() in valid_extensions:
        return f".{original_extension.lower()}"
    else:
        return f".{original_extension.lower()}"


def clean_filename(filename):
    """Clean filename to remove invalid characters and normalize spacing."""
    # Defined invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    # Replacing
    filename = filename.replace(' ', '_')
    filename = filename.replace(',', '')
    filename = filename.replace('/', '-')
    return filename


def construct_filename(text, url, file_type):
    """Construct a proper filename with the correct extension."""
    # Clean the text-based filename
    base_filename = clean_filename(text)
    # Getting extension from filename
    extension = get_file_extension(url, file_type)
    # Combine filename and extension
    return f"{base_filename}{extension}"


def check_downloaded_file(download_dir):
    """Check the most recently downloaded file in the specified directory."""
    try:
        files = [os.path.join(download_dir, f) for f in os.listdir(download_dir)]
        latest_file = max(files, key=os.path.getctime)
        print(f"Latest downloaded file: {latest_file}")
        return latest_file
    except ValueError:
        print("No files downloaded in the directory.")
        return None


def download_file(url, filename):
    """Download file using Selenium for complex JavaScript-based redirection or direct download."""
    download_dir = os.path.dirname(filename)
    try:
        if 'webarchive' in url or 'web.archive' in url:
            print('This is likely a webarchive URL, using Selenium library...')
            latest_file = handle_webarchive_download(url, download_dir,
                                                     check_downloaded_file)  # calling function from different file
            if latest_file:
                os.rename(latest_file, filename)
                print(f"Successfully downloaded and renamed to {filename}")
                return True
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            print("Non-webarchive file, direct download method using 'urllib.request' library)...")
            response = requests.get(url, headers=headers, allow_redirects=True)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"Successfully downloaded {filename}")
                return True
        return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False


def validate_id_input(selected_files, max_id):
    """Validate user input for file selection."""
    selected_ids = []
    try:
        if selected_files.lower() == 'all':
            return list(range(1, max_id + 1))

        for part in selected_files.split(','):
            if '-' in part:
                start, end = map(int, part.split('-'))
                if start < 1 or end > max_id:
                    print(f"ID range {start}-{end} out of range. Please enter IDs between 1 and {max_id}")
                    return None
                selected_ids.extend(range(start, end + 1))
            else:
                id_num = int(part)
                if id_num < 1 or id_num > max_id:
                    print(f"ID {id_num} out of range. Please enter IDs between 1 and {max_id}")
                    return None
                selected_ids.append(id_num)
        return selected_ids
    except ValueError:
        print("Invalid input format. Please use numbers separated by commas or ranges (e.g., 1,3,5 or 1-3)")
        return None


def process_url(dataurl, raw_data_dir):
    """Process a single URL: Extract file links and prompt user for download."""
    failed_downloads = []

    try:
        print(f"Reading webpage: {dataurl}")
        page = urlopen(Request(dataurl, headers={'User-Agent': 'Mozilla/5.0'}))
        soup = BeautifulSoup(page, "html.parser")

        links = []
        for link in soup.find_all('a', href=True):
            file_type = link['href'].split('.')[-1].lower()
            link_text = link.get_text(strip=True).lower()
            if file_type in ["xls", "xlsx"]:
                full_url = urljoin(dataurl, link['href'])
                link_text = link.get_text(strip=True)
                filename = construct_filename(link_text, full_url, file_type)
                links.append((full_url, filename, link_text))

        if not links:
            print(f"No downloadable files found on {dataurl}\n")
            return

        # Display available files
        print("\nAvailable files:")
        for i, (url, filename, text) in enumerate(links, 1):
            print(f"{i}. {text}  →  {filename}\n   URL: {url}\n")
        print("*" * 50)

        # User input for IDs
        while True:
            selected_files = input(f"Select files from {dataurl} (IDs, range, or 'all'): ")
            selected_ids = validate_id_input(selected_files, len(links))
            if selected_ids is not None:
                break
            print("Invalid selection, please try again.\n")

        # Download selected files
        for id in selected_ids:
            url, filename, text = links[id - 1]
            full_path = os.path.join(raw_data_dir, filename)

            if os.path.isfile(full_path):
                print(f"File {filename} already exists, skipping.\n")
                continue

            print(f"Downloading {filename} ...\n")
            success = download_file(url, full_path)
            if not success:
                failed_downloads.append((id, filename, url))
            time.sleep(1)

        # Failed downloads report
        if failed_downloads:
            print("\nFailed downloads:")
            for id, filename, url in failed_downloads:
                print(f"{id}. {filename} → {url}\n")

    except URLError as e:
        print(f"Network error on {dataurl}: {e}")
    except Exception as e:
        print(f"Unexpected error on {dataurl}: {e}")


### MAIN EXECUTION
def main():
    # Defining directories
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
    except NameError:
        BASE_DIR = Path.cwd()
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "critical-care-beds" / "after-2010")
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    # Defining URL with data
    urls = [
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-data-2010-11/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-data-2011-12/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-capacity-data-2012-13/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2013-14-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2014-15-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2015-16-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2016-17-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2017-18-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2018-19-data/",
        "https://www.england.nhs.uk/statistics/statistical-work-areas/critical-care-capacity/critical-care-bed-capacity-and-urgent-operations-cancelled-2019-20-data/"
    ]
    failed_downloads = []  # list of failed downloads

    for url in urls:
        process_url(url, RAW_DATA_DIR)


if __name__ == "__main__":
    main()




