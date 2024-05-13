import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from utensils import navigate_to_category, get_downloaded_filename, download_file

# initiate logging at info level
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\weekly"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.implicitly_wait(5)
driver.maximize_window()

categories = [
    ('View Further mobility data','slnid-line', 0),
    ('View Service Points Master Data','bfr-rollstuhl', 1,),
    ('View Service Points Master Data','prm-toilet-full', 0),
    ('View Service Points Master Data','prm-toilet-actual-date', 0),
    ('View Service Points Master Data','prm-stop_point-actual-date', 0),
    ('View Service Points Master Data','prm-platform-actual-date', 0),
    ('View Service Points Master Data','bav_liste', 1),
    ("View Business organisations", "business-organisations", 0)
]

for category_name, dataset_link, file_position in categories:
    navigate_to_category(driver, category_name)
    try:
        dataset_element = WebDriverWait(driver,20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
        download_file(driver, file_position)
        filename = get_downloaded_filename(driver)
        logging.info(f"Successfully downloaded file {filename}")
        time.sleep(5)
    except TimeoutException:
        page_2 = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/group/didok-group?page=2"]')))
        page_2.click()

        dataset_element = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
        download_file(driver, file_position)
        logging.info(f"Successfully downloaded file {filename}")
        time.sleep(5)
    time.sleep(2)
    driver.execute_script("window.history.go(-1)")

time.sleep(5)
driver.quit()