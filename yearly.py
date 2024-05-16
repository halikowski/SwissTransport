import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from utensils import get_downloaded_filename, download_file, snowsql_ingest
import snowflake.connector
import pandas as pd
import os
from raw import load_raw_occupancy_data

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
sf_session = get_snowpark_session()

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\yearly"}
chrome_options.add_experimental_option("detach",True)
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://opentransportdata.swiss/en/group")
driver.implicitly_wait(5)
driver.maximize_window()

files_directory = r'C:/Users/Mateusz/Downloads/transport/yearly'

# Downloading Passengers boarding and alighting.xlsx file (old name frequentia)
try:
    further_mob_data = WebDriverWait(driver,20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'a[title="View Further mobility data"]')))
    further_mob_data.click()

    occupancy_data = WebDriverWait(driver,20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/dataset/einundaus"]')))
    occupancy_data.click()
    download_file(driver, 0)
    filename = get_downloaded_filename(driver)
    logging.info(f'Successfully downloaded file {filename}')
except (TimeoutException, NoSuchElementException) as e:
    logging.error('An error occured while locating or clicking elements:', e)

df = pd.read_excel(f'{files_directory}/{filename}')
filename = 'Occupancy_data.csv'
df.to_csv(filename, index=False)

file_path = os.path.join(files_directory, filename)
if os.path.exists(file_path):
    try:
        snowsql_ingest(files_directory, filename, 'yearly/occupancy_files')
        logging.info(f'Successfully uploaded {filename} to my_stg/yearly/occupancy_fles')
    except Exception as e:
        logging.error(f'Error occured during uploading {filename} to internal stage.', e)
# copy into table raw_occupancy_data
load_raw_occupancy_data(sf_session)
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
