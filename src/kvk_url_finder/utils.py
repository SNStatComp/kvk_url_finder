import logging
import re
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pickle
from kvk_url_finder import LOGGER_BASE_NAME, CACHE_DIRECTORY

# regular expressions
ZIPCODE_REGEXP = "(\d{4}\s{0,1}[a-zA-Z]{2})"
ZIPCODE_REGEXPC = re.compile(ZIPCODE_REGEXP)


def standard_zipcode(zip_codes):
    """
    Make a clean list of zip codes
    :param zip_codes: string list with zip codes
    :return:  list with cleaned zip codes
    """
    zips = [re.sub(r"\s+", "", zc).upper() for zc in zip_codes]
    return set(zips)


def cache_to_disk(func):
    def wrapper(*args):
        cache_file = '{}{}.pkl'.format(func.__name__, args).replace('/', '_')
        cache = Path(CACHE_DIRECTORY) / cache_file

        try:
            with open(cache, 'rb') as f:
                return pickle.load(f)
        except IOError:
            result = func(*args)
            with open(cache, 'wb') as f:
                pickle.dump(result, f)
            return result

    return wrapper


@cache_to_disk
def get_page_from_url(url):
    page = requests.get(url)
    return page


class UrlAnalyse(object):

    def __init__(self, url, store_page_to_cache=True):

        self.logger = logging.getLogger(LOGGER_BASE_NAME)

        self.store_page_to_cache = store_page_to_cache
        if not url.startswith('http://') and not url.startswith('https://'):
            self.url = 'http://{:s}/'.format(url)
        else:
            self.url = url

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

        if self.store_page_to_cache:
            page = get_page_from_url(self.url)
        else:
            page = requests.get(self.url)

        if page.status_code != 200:
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
