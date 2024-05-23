import os
import time
import logging
import zipfile
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from snowflake.snowpark import Session


# Import custom functions from utensils file
from scripts.utensils import (
    navigate_to_category, download_file, get_downloaded_filename, snowsql_ingest,
    setup_selenium_driver
)


def download_and_process_file(category_name: str,
                              dataset_link: str,
                              file_position: int,
                              file_format: str,
                              files_directory: str
                              ) -> str:
    """
        Takes arguments specified in config.json file and navigates to proper category on data supplier's website in order
        to find the specified file and download it to specified directory.
        Depending on the file format, some processing might be made to remove excess rubbish rows and/or change
        the file's name or format
    """
    driver = setup_selenium_driver(files_directory)
    navigate_to_category(driver, category_name)
    try:
        dataset_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
    except TimeoutException:  # Try finding the element on 2nd page
        page_2 = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/group/didok-group?page=2"]')))
        page_2.click()
        dataset_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()

    # Once element is located, download the file
    download_file(driver, file_position)
    time.sleep(10)
    # Get the file name
    filename = get_downloaded_filename(files_directory)
    logging.info(f"Successfully downloaded file {filename}")
    time.sleep(10)
    # Return to previous site
    driver.execute_script("window.history.go(-1)")

    # File format handling

    # Most files are downloaded in .zip format, which when unzipped contains only one file - in format .csv.zip
    # File extension is changed from .csv.zip to .csv once unpacked.
    if file_format == 'zip':
        zip_file = f'{files_directory}/{filename}'
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(files_directory)
        filename = filename.replace('.zip', '')
    elif file_format == 'xlsx':
        # This file data is placed in 2nd sheet named 'Data'
        if filename == 'Passengers boarding and alighting.xlsx':
            df = pd.read_excel(os.path.join(files_directory, filename), sheet_name="Data")
        # This file requires removing 3 rubbish text rows
        elif filename == 'BAV_List_current_timetable.xlsx':
            df = pd.read_excel(os.path.join(files_directory, filename))
            rows_to_drop = [0,1,2]
            df.drop(rows_to_drop, inplace=True)
        # Opening other files without changes
        else:
            df = pd.read_excel(os.path.join(files_directory, filename))
        # Exporting as .csv
        filename = filename.replace('.xlsx', '.csv')
        filename = filename.replace(' ','_')
        df.to_csv(os.path.join(files_directory, filename), index=False, sep=';')
    # Removing unnecessary spaces inside the file name
    else:
        filename = filename.replace(' ', '')

    driver.quit()
    return filename


def ingest_file_to_snowflake(filename: str, files_directory: str, destination: str) -> None:
    """
    This function uses SnowSQL CLI tool for connecting to snowflake and ingesting the file to specified folder
    at internal stage, using PUT command.
    Login to SnowSQL happens automatically due to ENV variables specified in .env file and docker-entrypoint.sh file.
    """
    file_path = os.path.join(files_directory, filename)
    if os.path.exists(file_path):
        try:
            snowsql_ingest(files_directory, filename, destination)
            logging.info(f'Successfully uploaded {filename} to my_stg/{destination}')
        except Exception as e:
            logging.error(f'Error occurred during uploading {filename} to internal stage.', e)


def load_data_to_table(sf_session: Session, load_func, destination: str) -> None:
    """
    This function uses Snowflake and casts appropriate loading function (written with Snowpark package).
    It results in data copied from internal stage's folder into transient tables in database's raw schema.
    """
    try:
        load_func(sf_session)
        logging.info(f'Successfully copied data from {destination} to raw table')
    except Exception as e:
        logging.error(f'Error occurred during copying data from {destination} to raw table:', e)


def cleanup_file(file_path: str) -> None:
    """
    This function removes file from the specified file path.
    """
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f'Successfully removed file {file_path}')
        except Exception as e:
            logging.error(f'Error removing file {file_path}: {e}')
