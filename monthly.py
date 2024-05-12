import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# initiate logging at info level
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

def navigate_to_category(category_title):
    """Goes to the transport category selection page"""
    driver.get("https://opentransportdata.swiss/en/group")
    category_link = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f'a[title="{category_title}"]')))
    category_link.click()


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


def get_downloaded_filename():
    """Gets name of the downloaded file by opening Chrome downloads window and reading last downloaded file name"""
    driver.execute_script("window.open()")
    # new tab
    driver.switch_to.window(driver.window_handles[-1])
    # navigate to chrome downloads
    driver.get('chrome://downloads')
    while True:
        try:
            # get downloaded percentage
            downloadPercentage = driver.execute_script(
                "return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('#progress').value")
            # check if downloadPercentage is 100 (otherwise keep waiting)
            if downloadPercentage == 100:
                # return the file name once the download is completed
                time.sleep(1)
                return driver.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
            else:
                continue
        except:
            logging.error('An error occured while retrieving the filename')
            break


# Driver setup
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\Mateusz\Downloads\transport\monthly"}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("detach",True)
driver = webdriver.Chrome(options=chrome_options)
driver.implicitly_wait(5)
driver.maximize_window()

categories = [
    ('View Further mobility data','bike-parking', 0),
    ('View Further mobility data','vm-liste', 0),
    ('View Further mobility data','parking-facilities', 0),
    ('View Further mobility data','vm-liste', 1)
]

for category_name, dataset_link, file_position in categories:
    navigate_to_category(category_name)
    try:
        dataset_element = WebDriverWait(driver,20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
        download_file(file_position)
        filename = get_downloaded_filename()
        logging.info(f"Successfully downloaded file {filename}")
    except TimeoutException:
        page_2 = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="/en/group/didok-group?page=2"]')))
        page_2.click()

        dataset_element = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'a[href="/en/dataset/{dataset_link}"]')))
        dataset_element.click()
        download_file(file_position)
        filename = get_downloaded_filename()
        logging.info(f"Successfully downloaded file {filename}")
    time.sleep(2)
    driver.execute_script("window.history.go(-1)")

time.sleep(5)
driver.quit()
