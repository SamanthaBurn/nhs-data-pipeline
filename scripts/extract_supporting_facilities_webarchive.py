import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def setup_chrome_driver(download_dir):
    driver_path = ChromeDriverManager().install()
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True
    })
    
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

def download_file(url):
    download_dir = os.path.abspath(os.getcwd())  # Get absolute path
    try:
        driver = setup_chrome_driver(download_dir)
        print(f"Attempting download from {url}")
        driver.get(url)
        
        # Wait for potential redirects
        time.sleep(5)
        
        # Check if file appears in download directory
        initial_files = set(os.listdir(download_dir))
        time.sleep(10)  # Wait for download
        final_files = set(os.listdir(download_dir))
        
        new_files = final_files - initial_files
        if new_files:
            print(f"Downloaded: {new_files}")
        else:
            print("No new files detected")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    url = "http://webarchive.nationalarchives.gov.uk/20130107105354/http://www.dh.gov.uk/prod_consum_dh/groups/dh_digitalassets/@dh/@en/@ps/@sta/@perf/documents/digitalasset/dh_103812.xls"
    download_file(url)