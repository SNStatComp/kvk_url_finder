import logging
import re
import requests
from requests.exceptions import (ConnectionError, ReadTimeout)
from bs4 import BeautifulSoup
from kvk_url_finder import LOGGER_BASE_NAME
from cbs_utils.misc import get_page_from_url

# regular expressions
ZIPCODE_REGEXP = "(\d{4}\s{0,1}[a-zA-Z]{2})"
ZIPCODE_REGEXPC = re.compile(ZIPCODE_REGEXP)


class UrlAnalyse(object):

    def __init__(self, url, store_page_to_cache=True, timeout=1.0):

        self.logger = logging.getLogger(LOGGER_BASE_NAME)

        self.store_page_to_cache = store_page_to_cache
        if not url.startswith('http://') and not url.startswith('https://'):
            self.url = 'http://{:s}/'.format(url)
        else:
            self.url = url

        self.timeout = timeout

        self.exists = False

        self.soup = None
        self.zip_codes = None
        self.kvk_numbers = None

        self.make_soup()

        if self.soup:
            self.zip_codes = self.get_patterns(r"(\d{4}\s{0,1}[a-zA-Z]{2})")
            self.kvk_numbers = self.get_patterns(r"(\d{7,8})")

    def make_soup(self):
        """ Analyse a page using bs4"""

        try:
            if self.store_page_to_cache:
                self.logger.debug("Get (cached) page: {}".format(self.url))
                page = get_page_from_url(self.url, timeout=self.timeout)
            else:
                self.logger.debug("Get page: {}".format(self.url))
                page = requests.get(self.url, timeout=self.timeout)
        except (ConnectionError, ReadTimeout) as err:
            self.logger.warning(err)
        else:
            if page is None or page.status_code != 200:
                self.logger.warning(f"Page not found: {self.url}")
            else:
                self.exists = True
                self.soup = BeautifulSoup(page.text, 'lxml')

    def get_patterns(self, pattern):

        pattern_comp = re.compile(pattern)

        matches = list()

        lines = self.soup.find_all(string=pattern_comp)
        for line in lines:
            match = pattern_comp.search(str(line))
            if bool(match):
                grp = match.group(1)
                matches.append(grp)

        return matches
