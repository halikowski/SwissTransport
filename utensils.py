import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import subprocess


def navigate_to_category(driver,category_title):
    """Goes to the transport category selection page"""
    driver.get("https://opentransportdata.swiss/en/group")
    time.sleep(5)
    try:
        cookies = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#onetrust-accept-btn-handler')))
        cookies.click()
    except:
        pass
    time.sleep(2)
    category_link = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f'a[title="{category_title}"]')))
    category_link.click()


def download_file(driver,n):
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


def get_downloaded_filename(driver):
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
    WebDriverWait(driver, 360, 1).until(chrome_downloads)  # returns list of downloaded file paths
    # get latest downloaded file name and path
    time.sleep(10)
    dl_filename = driver.execute_script("""
                return document.querySelector('downloads-manager')
                .shadowRoot.querySelector('#downloadsList downloads-item')
                .shadowRoot.querySelector('div#content  #file-link').text""")  # latest downloaded file from the list
    time.sleep(5)
    # Close the current tab (chrome downloads)
    if "chrome://downloads" in driver.current_url:
        driver.close()
    # Switch back to original tab
    driver.switch_to.window(driver.window_handles[0])
    return dl_filename


def snowsql_ingest(directory, filename, stg_folder):
    """ Runs SnowSQL commands for file ingestion into Snowflake's internal stage.
        Each file has it's own dedicated folder. """
    subprocess.run(['snowsql', '-q',
                    f"PUT file://{directory}/{filename} @my_stg/{stg_folder} auto_compress=true"])