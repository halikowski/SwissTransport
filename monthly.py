import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from utensils import navigate_to_category, download_file, get_downloaded_filename, snowsql_ingest
import snowflake.connector
import os
from raw import load_raw_bike_parking_data, load_raw_parking_data, load_raw_transport_types, load_raw_transport_subtypes

files_directory = r'C:/Users/Mateusz/Downloads/transport/monthly'

# initiate logging at info level
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

# snowflake connection
connection_parameters = {
    "account":"iigqpyy-qq30975",
    "user":"user_01",
    "password":"Snowp4rk",
    "role":"SYSADMIN",
    "database":"SWISS_TRANSPORT",
    "schema":"RAW",
    "warehouse":"TRANSPORT_WH",
    'login':'true'
    }

conn = snowflake.connector.connect(**connection_parameters)

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\monthly"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.implicitly_wait(5)
driver.maximize_window()

categories = [
    ('View Further mobility data','bike-parking', 0,'monthly/bike_files', load_raw_bike_parking_data),
    ('View Further mobility data','vm-liste', 0, 'monthly/tsub_files', load_raw_transport_subtypes),
    ('View Further mobility data','parking-facilities', 0, 'parking_files',load_raw_parking_data),
    ('View Further mobility data','vm-liste', 1,'tmode_files',load_raw_transport_types)
]

for category_name, dataset_link, file_position, destination, load_func  in categories:
    navigate_to_category(driver, category_name)
    try:
        dataset_element = WebDriverWait(driver,10).until(
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
    # ingest the downloaded file to snowflake internal stage
    file_path = os.path.join(files_directory, filename)
    if os.path.exists(file_path):
        try:
            snowsql_ingest(files_directory, filename, destination)
            logging.info(f'Successfully uploaded {filename} to my_stg/{destination}')
            load_func()
        except Exception as e:
            logging.error(f'Error occured during uploading {filename} to internal stage.', e)

# yet to add removal of the files from download folder as next step

conn.close()
time.sleep(5)
driver.quit()
