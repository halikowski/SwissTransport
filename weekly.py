import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from utensils import navigate_to_category, get_downloaded_filename, download_file, snowsql_ingest
import snowflake.connector
import zipfile
import pandas as pd
import os

files_directory = r'C:/Users/Mateusz/Downloads/transport/weekly'

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
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\weekly"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.implicitly_wait(5)
driver.maximize_window()

categories = [
    ('View Further mobility data','slnid-line', 0, 'zip','weekly/line_files'),
    ('View Service Points Master Data','bfr-rollstuhl', 1,'csv','weekly/bfr_files'),
    ('View Service Points Master Data','prm-toilet-full', 0, 'zip','weekly/full_toilet_files'),
    ('View Service Points Master Data','prm-toilet-actual-date', 0, 'zip','weekly/toilet_files'),
    ('View Service Points Master Data','prm-stop_point-actual-date', 0, 'zip','weekly/stop_files'),
    ('View Service Points Master Data','prm-platform-actual-date', 0, 'zip','weekly/platform_files'),
    ('View Service Points Master Data','bav_liste', 1, 'xlsx','weekly/bav_files'),
    ("View Business organisations", "business-organisations", 0, 'zip','weekly/org_files')
]

for category_name, dataset_link, file_position, file_format, destination in categories:
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
    if file_format == 'zip':
        zip_file = f'{files_directory}/{filename}'
        extract_to = files_directory
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        filename = filename.replace('.zip','')
    elif file_format == 'xlsx':
        df = pd.read_excel(f'{files_directory}/{filename}')
        df.drop(index=range(1,4), inplace=True)
        filename = 'BAV_List_current_timetable.csv'
        df.to_csv(filename, index=False)
    else:
        filename = filename.replace(' ','')
    # ingest the downloaded file to snowflake internal stage
    file_path = os.path.join(files_directory, filename)
    if os.path.exists(file_path):
        try:
            snowsql_ingest(files_directory, filename, destination)
            logging.info(f'Successfully uploaded {filename} to my_stg/{destination}')
        except Exception as e:
            logging.error(f'Error occured during uploading {filename} to internal stage.', e)

conn.close()
time.sleep(5)
driver.quit()