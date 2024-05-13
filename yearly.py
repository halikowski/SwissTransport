import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from utensils import get_downloaded_filename, download_file

# initiate logging at info level
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\yearly"}
chrome_options.add_experimental_option("detach",True)
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://opentransportdata.swiss/en/group")
driver.implicitly_wait(5)
driver.maximize_window()

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


time.sleep(5)
driver.quit()
