import datetime
import difflib
import multiprocessing as mp
import os
import re
import sys
import time

import Levenshtein
import pandas as pd
import progressbar as pb
import pytz
import tldextract
from tqdm import tqdm

from cbs_utils.misc import (create_logger, is_postcode, standard_postcode, print_banner)
from cbs_utils.web_scraping import (UrlSearchStrings, BTW_REGEXP, ZIP_REGEXP, KVK_REGEXP,
                                    get_clean_url)
from kvk_url_finder import LOGGER_BASE_NAME, CACHE_DIRECTORY
from kvk_url_finder.model_variables import COUNTRY_EXTENSIONS, SORT_ORDER_HREFS
from kvk_url_finder.models import *
from kvk_url_finder.utils import paste_strings

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

try:
    # if profile exist, it means we are running kernprof to time all the lines of the functions
    # decorated with #@profile
    # noinspection PyUnboundLocalVariable
    isinstance(profile, object)
except NameError:
    # in case this fails, we add the profile decorator to the builtins such that it does
    # not raise an error.
    import line_profiler
    import builtins

    profile = line_profiler.LineProfiler()
    builtins.__dict__["profile"] = profile

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

CACHE_TYPES = ["msg_pack", "hdf", "sql", "csv", "pkl"]
COMPRESSION_TYPES = [None, "zlib", "blosc"]
SCRAPERS = ["bs4", "scrapy"]

MAX_SQL_CHUNK = 500

STOP_FILE = "stop"

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]
PB_MESSAGE_FORMAT = " Processing {} of {}"


def progress_bar_message(cnt, total, kvk_nr=None, naam=None):
    msg = " {:4d}/{:4d}".format(cnt + 1, total)

    if kvk_nr is not None:
        message = "{:8d} - ".format(kvk_nr)
    else:
        message = "{:8s}   ".format(" " * 8)

    if naam is not None:
        naam_str = "{:20s}".format(naam)
    else:
        naam_str = "{:20s}".format(" " * 50)

    message += naam_str[:20]

    msg += ": {}".format(message)
    return msg


class UrlParser(mp.Process):
    """
    Class to parse a csv file and couple the unique kwk numbers to a list of urls

    Parameters
    ----------
    url_input_file_name: str
        Name of the input file with all the URL
    reset_database: bool
        Reset the data base file in case this flag is True
    maximum_entries: int
        Give the maximum number of entries to process. Default = None, which means all entries are
        used. For a finite number of entries the maximum number of rows read from the csv file is
        limited to 'maximum_entries'
    """

    def __init__(self,
                 database_name=None,
                 database_type=None,
                 store_html_to_cache=False,
                 internet_scraping=True,
                 search_urls=False,
                 max_cache_dir_size=None,
                 user=None,
                 password=None,
                 hostname=None,
                 address_keys=None,
                 kvk_url_keys=None,
                 maximum_entries=None,
                 start_url_index=None,
                 stop_url_index=None,
                 start_url=None,
                 stop_url=None,
                 progressbar=False,
                 singlebar=False,
                 n_url_count_threshold=100,
                 force_process=False,
                 kvk_range_read=None,
                 kvk_range_process=None,
                 impose_url_for_kvk=None,
                 threshold_distance=None,
                 threshold_string_match=None,
                 save=True,
                 number_of_processes=1,
                 i_proc=None,
                 log_file_base="log",
                 log_level_file=logging.DEBUG,
                 older_time: datetime.timedelta = None,
                 timezone: pytz.timezone = 'Europe/Amsterdam',
                 filter_urls: list = None
                 ):

        # launch the process
        console_log_level = logging.getLogger(LOGGER_BASE_NAME).getEffectiveLevel()
        if i_proc is not None and number_of_processes > 1:
            mp.Process.__init__(self)
            formatter = logging.Formatter("{:2d} ".format(i_proc) +
                                          "%(levelname)-5s : "
                                          "%(message)s "
                                          "(%(filename)s:%(lineno)s)",
                                          datefmt="%Y-%m-%d %H:%M:%S")
            log_file = "{}_{:02d}".format(log_file_base, i_proc)
            logger_name = f"{LOGGER_BASE_NAME}_{i_proc}"
            self.logger = create_logger(name=logger_name,
                                        console_log_level=console_log_level,
                                        file_log_level=log_level_file,
                                        log_file=log_file,
                                        formatter=formatter)
            self.logger.info("Set up class logger for proc {}".format(i_proc))
        else:
            self.logger = logging.getLogger(LOGGER_BASE_NAME)
            self.logger.setLevel(console_log_level)
            self.logger.info("Set up class logger for main {}".format(__name__))

        self.logger.debug("With debug on?")

        # a list of all country url extension which we want to exclude
        self.exclude_extension = pd.DataFrame(COUNTRY_EXTENSIONS,
                                              columns=["include", "country", "suffix"])
        self.exclude_extension = self.exclude_extension[~self.exclude_extension["include"]]
        self.exclude_extension = self.exclude_extension.set_index("suffix", drop=True).drop(
            ["include"], axis=1)

        self.i_proc = i_proc
        self.store_html_to_cache = store_html_to_cache
        self.max_cache_dir_size = max_cache_dir_size
        self.internet_scraping = internet_scraping
        self.search_urls = search_urls

        self.maximum_entries = maximum_entries
        self.start_url = start_url
        self.stop_url = stop_url
        self.force_process = force_process
        self.start_url_index = start_url_index
        self.stop_url_index = stop_url_index

        self.address_keys = address_keys
        self.kvk_url_keys = kvk_url_keys

        self.save = save
        self.older_time = older_time
        self.timezone = timezone
        self.filter_urls = filter_urls

        if progressbar:
            # switch off all logging because we are showing the progress bar via the print statement
            # logger.disabled = True
            # logger.disabled = True
            # logger.setLevel(logging.CRITICAL)
            for handle in self.logger.handlers:
                try:
                    getattr(handle, "baseFilename")
                except AttributeError:
                    # this is the stream handle because we get an AtrributeError. Set it to critical
                    handle.setLevel(logging.CRITICAL)

        self.url_df: pd.DataFrame = None
        self.addresses_df: pd.DataFrame = None
        self.kvk_df: pd.DataFrame = None

        self.company_vs_kvk = None
        self.n_company = None

        self.number_of_processes = number_of_processes

        self.kvk_ranges = None

        self.database = init_database(database_name, database_type=database_type,
                                      user=user, password=password, host=hostname)
        self.database.execute_sql("SET TIME ZONE '{}'".format(self.timezone))
        tables = init_models(self.database, self.reset_database)
        self.UrlNL = tables[0]
        self.company = tables[1]
        self.address = tables[2]
        self.website = tables[3]

    def export_db(self, file_name):
        export_file = Path(file_name)
        if export_file.suffix in (".xls", ".xlsx"):
            with pd.ExcelWriter(file_name) as writer:
                for cnt, table in enumerate([self.company, self.address, self.website]):
                    query = table.select()
                    df = pd.DataFrame(list(query.dicts()))
                    try:
                        df.set_index(KVK_KEY, inplace=True)
                    except KeyError:
                        pass
                    sheetname = table.__name__
                    self.logger.info(f"Appending sheet {sheetname}")
                    df.to_excel(writer, sheet_name=sheetname)
        elif export_file.suffix in (".csv"):
            for cnt, table in enumerate([self.company, self.address, self.website]):
                this_name = export_file.stem + "_" + table.__name__.lower() + ".csv"
                query = table.select()
                df = pd.DataFrame(list(query.dicts()))
                try:
                    df.set_index(KVK_KEY, inplace=True)
                except KeyError:
                    pass
                self.logger.info(f"Writing to {this_name}")
                df.to_csv(this_name)

    def get_url_list_per_process(self):
        """
        Get a list of kvk numbers in the query
        """
        query = (self.UrlNL.select(self.UrlNL.url, self.UrlNL.kvk_nummer, self.UrlNL.btw_nummer,
                                   self.UrlNL.datetime)
                 .order_by(self.company.url))
        url_to_process = list()
        number_in_range = 0
        start = self.start_url
        stop = self.stop_url
        if start is None:
            process_url = True
        else:
            process_url = False
        for q in query:
            if self.maximum_entries is not None and number_in_range >= self.maximum_entries:
                # maximum entries reached
                break
            url = q.url
            if start is not None and kvk < start or stop is not None and kvk > stop:
                # skip because is outside range
                continue
            number_in_range += 1
            if not self.force_process and q.core_id is not None:
                # skip because we have already processed this record and the 'force' option is False
                continue
            # we can processes this record, so add it to the list
            kvk_to_process.append(kvk)

        n_kvk = len(kvk_to_process)
        self.logger.debug(f"Found {n_kvk} kvk's to process with {number_in_range}"
                          " in range")

        # check the ranges
        if number_in_range == 0:
            raise ValueError(f"No kvk numbers where found in range {start} -- {stop}")
        if n_kvk == 0:
            raise ValueError(f"Found {number_in_range} kvk numbers in range {start} -- {stop}"
                             f"but none to be processed")

        if n_kvk < self.number_of_processes:
            raise ValueError(f"Found {number_in_range} kvk numbers in range {start} -- {stop} "
                             f"with {n_kvk} to process, with only {self.number_of_processes} cores")

        n_per_proc = int(n_kvk / self.number_of_processes) + n_kvk % self.number_of_processes
        self.logger.debug("Number of kvk's per process: {n_per_proc}")
        self.kvk_ranges = list()

        for i_proc in range(self.number_of_processes):
            if i_proc == self.number_of_processes - 1:
                kvk_list = kvk_to_process[i_proc * n_per_proc:]
            else:
                kvk_list = kvk_to_process[i_proc * n_per_proc:(i_proc + 1) * n_per_proc]

            try:
                logger.info("Getting range")
                kvk_first = kvk_list[0]
                kvk_last = kvk_list[-1]
            except IndexError:
                logger.warning("Something is worong here")
            else:
                self.kvk_ranges.append(dict(start=kvk_first, stop=kvk_last))


