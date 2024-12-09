##########################################

# This python script will extract the links of datasets saved in .pdf, .xls, .xslx, .csv, from a given URL
# The script allows the selection of specific datasets in the URL and saved the selected files to folder specified in RAW_DATA_DIR

##########################################


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
import mimetypes



### FUNCTIONS

def setup_chrome_driver(download_dir):
    """It is necessary to have a Chromedriver for the webarchived files,
    that use Javascript to be downloaded in the browser"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": download_dir, 
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing_for_trusted_sources_enabled": False,
        "safebrowsing.enabled": False # allows all downloads
    })    
    driver_path = ChromeDriverManager().install()
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

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

"""
def check_and_rename_downloaded_file(download_dir, expected_filename):
    try:
        files = [os.path.join(download_dir, f) for f in os.listdir(download_dir)]
        latest_file = max(files, key=os.path.getctime)
        print(f"Latest downloaded file: {latest_file}")
        
        # Determine the extension from the latest file
        _, latest_ext = os.path.splitext(latest_file)
        
        # Determine expected extension from the expected filename or infer from the file MIME type
        expected_ext = os.path.splitext(expected_filename)[1]
        if not expected_ext:  # If no extension in expected, infer from the MIME type
            mime_type, _ = mimetypes.guess_type(latest_file)
            expected_ext = mimetypes.guess_extension(mime_type) if mime_type else latest_ext

        # Rename the file if the extension is missing or incorrect
        if latest_ext.lower() != expected_ext.lower():
            new_filename = f"{os.path.splitext(expected_filename)[0]}{expected_ext}"
            new_file_path = os.path.join(download_dir, new_filename)
            os.rename(latest_file, new_file_path)
            print(f"Renamed to: {new_file_path}")
            return new_file_path
        return latest_file
    except ValueError:
        print("No files downloaded in the directory.")
        return None




def download_file(url, filename):
    expected_filename = filename
    download_dir = os.path.dirname(expected_filename)
    try:
        if 'webarchive' or 'web.archive' in url:
            print('This is likely a webarchive URL, please inspect file URL to confirm this.')
            driver = setup_chrome_driver(download_dir)
            driver.get(url)
            time.sleep(10)  # Adjust time for file to download
            driver.quit()
            latest_file = check_downloaded_file(download_dir)
            if latest_file and os.path.basename(latest_file) == os.path.basename(expected_filename):
                print(f"Successfully downloaded {filename}")
                return True
            else:
                print("File may not have downloaded correctly or filename differs.")
                print("Renaming file to correct filename")
                renamed_file = check_and_rename_downloaded_file(download_dir, os.path.basename(filename))
                return renamed_file is not None
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            print("Non-webarchive URL, download not attempted with Selenium.")
            response = requests.get(url, headers=headers, allow_redirects=True)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False
"""

def download_file(url, filename):
    """Download file using Selenium for complex JavaScript-based redirection or direct download."""
    download_dir = os.path.dirname(filename)
    try:
        if 'webarchive' in url or 'web.archive' in url:
            print('This is likely a webarchive URL, using Selenium library...')
            driver = setup_chrome_driver(download_dir)
            driver.get(url)
            time.sleep(10)
            driver.quit()
            
            latest_file = check_downloaded_file(download_dir)
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



### MAIN EXECUTION
def main():
    # Defining directories
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent
    except NameError:
        BASE_DIR = Path.cwd()
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "supporting-facilities")
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    # Defining URL with data
    dataurl = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancelled-elective-operations/supporting-facilities-data/"

    try:
        print("Reading webpage...")
        page = urlopen(Request(dataurl, headers={'User-Agent': 'Mozilla/5.0'}))
        soup = BeautifulSoup(page, features="html.parser")
                
        links = []
        for link in soup.find_all('a', href=True):
            file_type = link['href'].split('.')[-1].lower()
            if file_type in ["pdf", "xls", "xlsx", "csv"]:
                full_url = urljoin(dataurl, link['href'])
                link_text = link.get_text(strip=True)
                filename = construct_filename(link_text, full_url, file_type)
                links.append((full_url, filename, link_text))

        # Display available files
        for i, (url, filename, text) in enumerate(links, 1):
            print(f"{i}. Filename text: {text}")
            print(f"   Will be saved as: {filename}")
            print(f"   URL: {url}\n")
        print("*" * 50)
        
        # User input for IDs
        while True:            
            selected_files = input("Which files do you wish to download? Enter individual IDs before listed filename (e.g., 1,3,5), range of IDs(e.g., 4-7), combination of specific IDs and range (e.g. 1-3, 8) or all listed files by typing 'all': ")
    
            selected_ids = validate_id_input(selected_files, len(links))
            if selected_ids is not None:
                break
            print("Please try again.\n")
                
        # Output
        for id in selected_ids:
            url, filename, text = links[id - 1]
            full_path = os.path.join(RAW_DATA_DIR, filename)
            
            if os.path.isfile(full_path):
                print(f"File {filename} already exists, skipping.\n")
                continue
    
            print(f"Downloading {filename} ...\n")
            success = download_file(url, full_path)
            if success:
                print(f"Successfully downloaded {filename}\n")
            else:
                print(f"Failed to download {filename}\n")
            time.sleep(1)
        
    except URLError as e:
        print(f"Network error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
     
if __name__ == "__main__":
    main()
    
    

