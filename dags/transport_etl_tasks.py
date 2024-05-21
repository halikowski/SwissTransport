import os
import time
import logging
import zipfile
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from scripts.utensils import (
    navigate_to_category, download_file, get_downloaded_filename, snowsql_ingest,
    setup_selenium_driver
)


def download_and_process_file(category_name, dataset_link, file_position, file_format, files_directory):
    driver = setup_selenium_driver(files_directory)
    navigate_to_category(driver, category_name)

    try:
        dataset_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
    except TimeoutException:
        page_2 = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/group/didok-group?page=2"]')))
        page_2.click()
        dataset_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()

    download_file(driver, file_position)
    filename = get_downloaded_filename(files_directory)
    logging.info(f"Successfully downloaded file {filename}")
    time.sleep(10)
    driver.execute_script("window.history.go(-1)")

    # File format handling
    if file_format == 'zip':
        zip_file = f'{files_directory}/{filename}'
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(files_directory)
        filename = filename.replace('.zip', '')
    elif file_format == 'xlsx':
        df = pd.read_excel(os.path.join(files_directory, filename))
        if filename == 'BAV_List_current_timetable.xlsx':
            df.drop(index=range(1,4), inplace=True)
        filename = filename.replace('.xlsx', '.csv')
        df.to_csv(os.path.join(files_directory, filename), index=False)
    else:
        filename = filename.replace(' ', '')

    driver.quit()
    return filename


def ingest_file_to_snowflake(filename, files_directory, destination):
    file_path = os.path.join(files_directory, filename)
    if os.path.exists(file_path):
        try:
            snowsql_ingest(files_directory, filename, destination)
            logging.info(f'Successfully uploaded {filename} to my_stg/{destination}')
        except Exception as e:
            logging.error(f'Error occurred during uploading {filename} to internal stage.', e)


def load_data_to_table(sf_session, load_func, destination):
    try:
        load_func(sf_session)
        logging.info(f'Successfully copied data from {destination} to raw table')
    except Exception as e:
        logging.error(f'Error occurred during copying data from {destination} to raw table:', e)


def cleanup_file(file_path):
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f'Successfully removed file {file_path}')
        except Exception as e:
            logging.error(f'Error removing file {file_path}: {e}')
