import logging
import os
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.exceptions import (ConnectionError, ReadTimeout)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cbs_utils.misc import get_page_from_url
from kvk_url_finder import LOGGER_BASE_NAME

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


class UrlAnalyse(object):

    def __init__(self, url, store_page_to_cache=True, timeout=1.0, max_iterations=10):

        self.logger = logging.getLogger(LOGGER_BASE_NAME)

        self.store_page_to_cache = store_page_to_cache
        if not url.startswith('http://') and not url.startswith('https://'):
            self.url = 'http://{:s}/'.format(url)
        else:
            self.url = url

        self.max_iterations = max_iterations
        self.timeout = timeout
        self.session = requests.Session()

        self.exists = False

        self.zip_codes = list()
        self.kvk_numbers = list()
        self.number_of_iterations = 0

        self.recursive_pattern_search(self.url)

    def recursive_pattern_search(self, url):
        """
        Search the 'url'  for the patterns and continue of links to other pages are present
        """

        self.number_of_iterations += 1
        soup = self.make_soup(url)

        if soup:
            zip_codes = self.get_patterns(soup, r"(\d{4}\s{0,1}[a-zA-Z]{2})")
            kvk_numbers = self.get_patterns(soup, r"(\d{7,8})")
            self.zip_codes.extend(zip_codes)
            self.kvk_numbers.extend(kvk_numbers)

            frames = soup.find_all('frame')
            for frame in frames:
                src = frame.get('src')
                url = urljoin(url, src)
                if self.number_of_iterations <= self.max_iterations:
                    self.recursive_pattern_search(url)
                else:
                    self.logger.warning(
                        "Maximum number of {} iterations reached!".format(self.max_iterations))

    def make_soup(self, url):
        """ Analyse a page using bs4"""

        soup = None
        try:
            if self.store_page_to_cache:
                self.logger.debug("Get (cached) page: {}".format(url))
                page = get_page_from_url(url, timeout=self.timeout)
            else:
                self.logger.debug("Get page: {}".format(url))
                page = self.session.get(url, timeout=self.timeout)
        except (ConnectionError, ReadTimeout) as err:
            self.logger.warning(err)
        else:
            if page is None or page.status_code != 200:
                self.logger.warning(f"Page not found: {url}")
            else:
                self.exists = True
                soup = BeautifulSoup(page.text, 'lxml')

        return soup

    @staticmethod
    def get_patterns(soup, pattern):

        pattern_comp = re.compile(pattern)

        matches = list()

        lines = soup.find_all(string=pattern_comp)
        for line in lines:
            match = pattern_comp.search(str(line))
            if bool(match):
                grp = match.group(1)
                matches.append(grp)

        return matches


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
