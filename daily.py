import logging
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\daily"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://opentransportdata.swiss/en/group/actualdata-group")
driver.implicitly_wait(5)
driver.maximize_window()

def download_file(n):
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


# Downloading *_istdaten.csv file
try:
    actual_data = WebDriverWait(driver,20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/dataset/istdaten"]')))
    actual_data.click()
    logging.info('Successfully downloaded file *_istdaten.csv')
except (TimeoutException, NoSuchElementException) as e:
    logging.error('An error occured while locating or clicking elements:', e)

download_file(0)

time.sleep(5)
driver.quit()