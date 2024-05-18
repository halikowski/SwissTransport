import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from utensils import navigate_to_category, get_downloaded_filename, download_file, snowsql_ingest, remove_file, setup_selenium_driver, get_snowpark_session, connection_parameters
import snowflake.connector
import pandas as pd
import os
from raw import load_raw_occupancy_data
import zipfile

files_directory = r'C:/Users/Mateusz/Downloads/transport/yearly'

# initiate logging at info level
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

# snowflake connection
conn = snowflake.connector.connect(**connection_parameters)
sf_session = get_snowpark_session()

# Driver setup
driver = setup_selenium_driver(files_directory)
driver.get("https://opentransportdata.swiss/en/group")

categories = [
    ('View Further mobility data', 'einundaus', 0, 'xlsx', 'yearly/occupancy_files', load_raw_occupancy_data),
]

for category_name, dataset_link, file_position, file_format, destination, load_func in categories:
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
    filename = get_downloaded_filename(driver)
    logging.info(f"Successfully downloaded file {filename}")
    time.sleep(10)
    driver.execute_script("window.history.go(-1)")

    # File format handling
    if file_format == 'zip':
        zip_file = f'{files_directory}/{filename}'
        extract_to = files_directory
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        filename = filename.replace('.zip', '')
    elif file_format == 'xlsx':
        df = pd.read_excel(f'{files_directory}/{filename}')
        if filename == 'BAV_List_current_timetable.xlsx':
            df.drop(index=range(1,4), inplace=True)
        df.drop(index=range(1, 4), inplace=True)
        filename = filename.replace('xlsx', 'csv')
        df.to_csv(filename, index=False)
    else:
        filename = filename.replace(' ', '')

    # ingest the downloaded file to snowflake internal stage
    file_path = os.path.join(files_directory, filename)
    if os.path.exists(file_path):
        try:
            snowsql_ingest(files_directory, filename, destination)
            logging.info(f'Successfully uploaded {filename} to my_stg/{destination}')
            load_func()
        except Exception as e:
            logging.error(f'Error occured during uploading {filename} to internal stage.', e)

    # copy into table from internal stage
    try:
        load_func(sf_session)
        logging.info(f'Successfully copied data from {destination} to raw table')
    except Exception as e:
        logging.error(f'Error occured during copying data from {destination} to raw table:', e)

    # remove file from download folder
    remove_file(file_path)


conn.close()
time.sleep(5)
driver.quit()
