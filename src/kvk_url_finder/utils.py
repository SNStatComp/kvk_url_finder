import logging
import os
import re

from kvk_url_finder import LOGGER_BASE_NAME
logger = logging.getLevelName(LOGGER_BASE_NAME)

try:
    from selenium import webdriver
except ModuleNotFoundError:
    # we are not using it now anyway
    pass
else:
    from selenium.webdriver.chrome.options import Options

chrome_driver = os.environ.get("CHROMEDRIVER")
options = Options()
options.headless = True
options.disable_gpu = True
options.start_maximized = True
options.disable_infobars = True
options.disable_extensions = True
options.disable_dev_shm_usage = True
options.no_sandbox = True
DRIVER = webdriver.Chrome(chrome_driver, chrome_options=options)

# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.expected_conditions import (presence_of_element_located)
# from selenium.webdriver.support.wait import WebDriverWait
#
# regular expressions
ZIPCODE_REGEXP = "(\d{4}\s{0,1}[a-zA-Z]{2})"
ZIPCODE_REGEXPC = re.compile(ZIPCODE_REGEXP)


class UrlDynAnalyse(object):

    def __init__(self, url, store_page_to_cache=True, timeout=1.0):

        self.logger = logging.getLogger(LOGGER_BASE_NAME)

        self.store_page_to_cache = store_page_to_cache
        if not url.startswith('http://') and not url.startswith('https://'):
            self.url = 'http://{:s}/'.format(url)
        else:
            self.url = url

        self.timeout = timeout

        self.exists = False

        self.zip_codes = None
        self.kvk_numbers = None

        self.dynamic_scrape()

    def dynamic_scrape(self):

        pattern = r"(\d{4}\s{0,1}[a-zA-Z]{2})"
        pattern_comp = re.compile(pattern)
        # connect to the web browser
        DRIVER.get(self.url)
        # frames = result.find_element_by_class_name('frame')
        frames = DRIVER.find_elements_by_css_selector('frame')
        zips = list()
        for frame in frames:
            src = frame.get_attribute('src')
            if src:
                DRIVER.get(src)
                alldiv = DRIVER.find_elements_by_css_selector("div")
                for div in alldiv:
                    text = div.text
                    match = pattern_comp.search(str(text))
                    if bool(match):
                        grp = match.group(1)
                        zips.append(grp)

        self.logger.debug(f"selenium found {zips}")

        self.zip_codes.extend(zips)
