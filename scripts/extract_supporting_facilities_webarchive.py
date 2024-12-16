##########################################

# This python script uses ChromeDriver to help extract datasets saved in webarchives 
# It is called in extract_supportng_facilities_main.py

##########################################

# pip install webdriver_manager
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

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
        "safebrowsing.enabled": False
    })    
    driver_path = ChromeDriverManager().install()
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

def handle_webarchive_download(url, download_dir, check_downloaded_file):
    """Handle downloads specifically for webarchive URLs"""
    try:
        driver = setup_chrome_driver(download_dir)
        driver.get(url)
        time.sleep(10)
        driver.quit()
        return check_downloaded_file(download_dir)
    except Exception as e:
        print(f"Selenium error: {e}")
        return None