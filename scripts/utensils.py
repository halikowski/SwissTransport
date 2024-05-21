import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import subprocess
from snowflake.snowpark import Session
import os
import logging
import glob
from dotenv import load_dotenv


load_dotenv('/opt/airflow/.env')
connection_parameters = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "role": os.getenv('SNOWFLAKE_ROLE'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "schema": os.getenv('SNOWFLAKE_SCHEMA'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    'login': 'true'
}

def remove_file(path: str):
    """
    Function for deleting the file from specified file_path.
    Used once the file is successfully uploaded to desired location
    """
    try:
        os.remove(path)
        print(f"File '{path}' successfully deleted.")
    except FileNotFoundError:
        print(f"File '{path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")



def setup_selenium_driver(dl_directory: str):
    """
    Function for quick Selenium driver setup, with specified file download directory.
    Returns driver object for further operations.
    """
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    prefs = {"download.default_directory": dl_directory,
             "download.prompt_for_download": False,}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.implicitly_wait(5)
    driver.maximize_window()
    logging.info(f'Selenium driver successfully configured with download directory: {dl_directory}')
    return driver

def navigate_to_category(driver,category_title):
    """Goes to the transport category selection page"""
    driver.get("https://opentransportdata.swiss/en/group")
    time.sleep(5)
    try:
        cookies = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#onetrust-accept-btn-handler')))
        cookies.click()
    except:
        pass
    time.sleep(2)
    category_link = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f'a[title="{category_title}"]')))
    category_link.click()


def download_file(driver,n):
    """ Downloads the desired file by clicking on the 'Explore' button and 'Download' button afterwards.
        The n argument specifies the position of the buttons, since some categories have multiple files available
        for download. n=0 -> first file on the list, n=1 -> second file, etc."""

    try:
        explore = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#dropdownExplorer')))
        explore[n].click()
        download = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.dropdown-item.resource-url-analytics')))
        download[n].click()
    except (TimeoutException, NoSuchElementException) as e:
        print('An error occured while downloading files:', e)


def get_downloaded_filename(download_dir):
    """Gets name of the downloaded file by opening Chrome downloads window and reading last downloaded file name"""
    start_time = time.time()
    time.sleep(10)
    while True:
        list_of_files = glob.glob(os.path.join(download_dir, '*'))
        if not list_of_files:
            continue

        latest_file = max(list_of_files, key=os.path.getctime)
        if not latest_file.endswith('.crdownload'):
            return os.path.basename(latest_file)

        if time.time() - start_time > 1200:
            raise TimeoutError("Timed out waiting for the download to complete.")

        time.sleep(10)


def snowsql_ingest(directory, filename, stg_folder):
    """ Runs SnowSQL commands for file ingestion into Snowflake's internal stage.
        Each file has it's own dedicated folder. """
    subprocess.run(['snowsql', '-q',
                    f"PUT file://{directory}/{filename} @my_stg/{stg_folder} auto_compress=true"])

def get_snowpark_session() -> Session:
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()