import logging
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utensils import download_file, get_downloaded_filename, snowsql_ingest, get_snowpark_session
import snowflake.connector
from raw import load_raw_transport

# basic logging config
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

files_directory = r'C:/Users/Mateusz/Downloads/transport/daily'

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\daily"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://opentransportdata.swiss/en/group/actualdata-group")
driver.implicitly_wait(5)
driver.maximize_window()

try:
    cookies = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, '#onetrust-accept-btn-handler')))
    cookies.click()
except:
    pass

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
sf_session = get_snowpark_session()

# Downloading *_istdaten.csv file
try:
    actual_data = WebDriverWait(driver,20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/dataset/istdaten"]')))
    actual_data.click()
    download_file(driver,0)
    filename = get_downloaded_filename(driver)
    logging.info(f'Successfully downloaded file {filename}')
except Exception as e:
    logging.error(f'An error occured while downloading file {filename}:',e)

# ingesting data to snowflake internal stage
file_path = os.path.join(files_directory, filename)
if os.path.exists(file_path):
    try:
        snowsql_ingest(files_directory, filename, 'daily')
        logging.info(f'Successfully uploaded {filename} to my_stg/daily')
    except Exception as e:
        logging.error(f'Error occured during uploading {filename} to internal stage.', e)
# copy into table raw_transport_data
load_raw_transport(sf_session)
# remove file from download folder
try:
    os.remove(file_path)
    print(f"File '{file_path}' successfully deleted.")
except FileNotFoundError:
    print(f"File '{file_path}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")

conn.close()
time.sleep(5)
driver.quit()
