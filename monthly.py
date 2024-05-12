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
    def chrome_downloads(drv):
        """Function to wait for all current chrome downloads to finish"""
        if not "chrome://downloads" in drv.current_url:  # if 'chrome downloads' is not current tab
            drv.execute_script("window.open('');")  # open a new tab
            drv.switch_to.window(driver.window_handles[1])  # switch to the new tab
            drv.get("chrome://downloads/")  # navigate to chrome downloads
        return drv.execute_script("""
                    return document.querySelector('downloads-manager')
                    .shadowRoot.querySelector('#downloadsList')
                    .items.filter(e => e.state === 'COMPLETE')
                    .map(e => e.filePath || e.file_path || e.fileUrl || e.file_url);
                    """)
    # wait for all the downloads to be completed
    WebDriverWait(driver, 120, 1).until(chrome_downloads)  # returns list of downloaded file paths
    # get latest downloaded file name and path
    time.sleep(5)
    dl_filename = driver.execute_script("""
                return document.querySelector('downloads-manager')
                .shadowRoot.querySelector('#downloadsList downloads-item')
                .shadowRoot.querySelector('div#content  #file-link').text""")  # latest downloaded file from the list
    # Close the current tab (chrome downloads)
    if "chrome://downloads" in driver.current_url:
        driver.close()
    # Switch back to original tab
    driver.switch_to.window(driver.window_handles[0])
    return dl_filename


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
        time.sleep(5)
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
        time.sleep(5)
    time.sleep(2)
    driver.execute_script("window.history.go(-1)")

time.sleep(5)
driver.quit()
