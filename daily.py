import logging
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utensils import download_file, get_downloaded_filename

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\daily"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://opentransportdata.swiss/en/group/actualdata-group")
driver.implicitly_wait(5)
driver.maximize_window()


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

time.sleep(5)
driver.quit()