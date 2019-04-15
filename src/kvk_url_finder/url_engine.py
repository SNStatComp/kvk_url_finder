import datetime
import multiprocessing as mp
import os
import re
import sys
import time

import pandas as pd
import progressbar as pb
import pytz
import tldextract
from tqdm import tqdm

from cbs_utils.misc import (create_logger, print_banner, standard_postcode)
from cbs_utils.web_scraping import (UrlSearchStrings, BTW_REGEXP, ZIP_REGEXP, KVK_REGEXP,
                                    get_clean_url)
from kvk_url_finder import LOGGER_BASE_NAME
from kvk_url_finder.model_variables import COUNTRY_EXTENSIONS, SORT_ORDER_HREFS
from kvk_url_finder.models import *
from kvk_url_finder.utils import (Range, check_if_url_needs_update, paste_strings)

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
                 force_process=False,
                 url_range_process=None,
                 save=True,
                 number_of_processes=1,
                 exclude_extensions=None,
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

        self.progressbar = progressbar
        self.showbar = progressbar
        if singlebar and i_proc > 0 or i_proc is None:
            # in case the single bar option is given, we only show the bar of the first process
            self.showbar = False

        # a list of all country url extension which we want to exclude
        self.exclude_extensions = pd.DataFrame(COUNTRY_EXTENSIONS,
                                               columns=["include", "country", "suffix"])
        self.exclude_extensions = self.exclude_extensions[~self.exclude_extensions["include"]]
        self.exclude_extensions = self.exclude_extensions.set_index("suffix", drop=True).drop(
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

        self.url_range_process = Range(url_range_process)
        self.url_ranges = None

        self.database = init_database(database_name, database_type=database_type,
                                      user=user, password=password, host=hostname)
        self.database.execute_sql("SET TIME ZONE '{}'".format(self.timezone))
        tables = init_models(self.database)
        self.UrlNL = tables[0]
        self.company = tables[1]
        self.address = tables[2]
        self.website = tables[3]

    def run(self):
        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.logger.debug(f"Scraping the url in range {self.url_range_process}")
        self.scrape_the_urls()

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
                 .order_by(self.UrlNL.url))
        url_to_process = list()
        number_in_range = 0
        if self.start_url is None:
            process_url = True
        else:
            process_url = False
        for q in query:
            if self.maximum_entries is not None and number_in_range >= self.maximum_entries:
                # maximum entries reached
                break
            url = q.url
            if self.start_url is not None and url == self.start_url:
                process_url = True
            if self.stop_url is not None and url == self.stop_url:
                process_url = False

            if not process_url:
                continue

            now = datetime.datetime.now(pytz.timezone(self.timezone))
            processing_time = q.datetime
            url_needs_update = check_if_url_needs_update(processing_time=processing_time,
                                                         current_time=now,
                                                         older_time=self.older_time)
            if url_needs_update or self.force_process:
                logger.debug(f"Adding {url} to the list")
                # we can processes this record, so add it to the list
                url_to_process.append(url)
                number_in_range += 1

        n_url = len(url_to_process)
        self.logger.debug(f"Found {n_url} kvk's to process with {number_in_range}"
                          " in range")

        # check the ranges
        if number_in_range == 0:
            raise ValueError(f"No urls numbers found in range {self.start_url} -- {self.stop_url}")
        if n_url == 0:
            raise ValueError(f"Found {number_in_range} urls in rang {self.start_url} -- "
                             f"{self.stop_url} but none to be processed")

        if n_url < self.number_of_processes:
            raise ValueError(f"Found {number_in_range} urls in range {self.start_url} -- "
                             f"{self.stop_url} with {n_url} to process, with only "
                             f"{self.number_of_processes} cores")

        n_per_proc = int(n_url / self.number_of_processes) + n_url % self.number_of_processes
        self.logger.debug("Number of urls's per process: {n_per_proc}")
        self.url_ranges = list()

        for i_proc in range(self.number_of_processes):
            if i_proc == self.number_of_processes - 1:
                url_list = url_to_process[i_proc * n_per_proc:]
            else:
                url_list = url_to_process[i_proc * n_per_proc:(i_proc + 1) * n_per_proc]

            try:
                logger.info("Getting range")
                url_first = url_list[0]
                url_last = url_list[-1]
            except IndexError:
                logger.warning("Something is wrong here")
            else:
                self.url_ranges.append(dict(start=url_first, stop=url_last))

    def scrape_the_urls(self):

        start = self.url_range_process.start
        stop = self.url_range_process.stop
        self.logger.info("Start finding best matching urls for proc {}".format(self.i_proc))

        if start is not None or stop is not None:
            if start is None:
                self.logger.info("Make query from start until stop {}".format(stop))
                query = self.UrlNL.select().where(self.UrlNL.url <= stop)
            elif stop is None:
                self.logger.info("Make query from start {} until end".format(start))
                query = self.UrlNL.select().where(self.UrlNL.url >= start)
            else:
                self.logger.info("Make query from start {} until stop {}".format(start, stop))
                query = self.UrlNL.select().where(self.UrlNL.url.between(start, stop))
                self.logger.info("Done!")
        else:
            self.logger.info("Make query without selecting in the kvk range")
            query = self.UrlNL.select()

        # count the number of none-processed queries (ie in which the processed flag == False
        # we have already imposed the max_entries option in the selection of the ranges
        self.logger.info("Counting all...")
        now = datetime.datetime.now(pytz.timezone(self.timezone))
        older = self.older_time
        max_queries = [check_if_url_needs_update(q.datetime, now, older) or
                       self.force_process for q in query].count(True)
        self.logger.info("Maximum queries obtained from selection as {}".format(max_queries))

        self.logger.info("Start processing {} queries between {} - {} ".format(max_queries,
                                                                               start, stop))

        if self.progressbar and self.showbar:
            pbar = tqdm(total=max_queries, position=self.i_proc, file=sys.stdout)
            pbar.set_description("@{:2d}: ".format(self.i_proc))
        else:
            pbar = None

        start = time.time()
        for cnt, q_url in enumerate(query):

            # first check if we do not have to stop
            if self.maximum_entries is not None and cnt == self.maximum_entries:
                self.logger.info("Maximum entries reached")
                break
            if os.path.exists(STOP_FILE):
                self.logger.info("Stop file found. Quit processing")
                os.remove(STOP_FILE)
                break

            if not check_if_url_needs_update(q_url.datetime, now, older):
                continue

            url = q_url.url

            if self.filter_urls and url not in self.filter_urls:
                logger.debug(f"filter urls is given so skip {url}")
                continue

            print_banner(f"Processing {url}")

            # quick check if we can processes this url based on the country code
            url_extract = tldextract.extract(url)
            suffix = url_extract.suffix
            if suffix in self.exclude_extensions.index:
                logger.info(f"Web site {url} has suffix '.{suffix}' Skipping")
                continue

            q_url.datetime = now
            q_url.suffix = url_extract.suffix
            q_url.subdomain = url_extract.subdomain
            q_url.domain = url_extract.domain

            url_analyse = UrlSearchStrings(url,
                                           search_strings={
                                               POSTAL_CODE_KEY: ZIP_REGEXP,
                                               KVK_KEY: KVK_REGEXP,
                                               BTW_KEY: BTW_REGEXP
                                           },
                                           sort_order_hrefs=SORT_ORDER_HREFS,
                                           stop_search_on_found_keys=[BTW_KEY],
                                           store_page_to_cache=self.store_html_to_cache,
                                           max_cache_dir_size=self.max_cache_dir_size,
                                           scrape_url=True
                                           )
            logger.debug("We scraped the web site. Store the social media")
            sm_list = list()
            ec_list = list()
            all_social_media = [sm.lower() for sm in SOCIAL_MEDIA]
            all_ecommerce = [ec.lower() for ec in PAY_OPTIONS]
            for external_url in url_analyse.external_hrefs:
                dom = tldextract.extract(external_url).domain
                if dom in all_social_media and dom not in sm_list:
                    logger.debug(f"Found social media {dom}")
                    sm_list.append(dom)
                if dom in all_ecommerce and dom not in ec_list:
                    logger.debug(f"Found ecommerce {dom}")
                    ec_list.append(dom)
            if ec_list:
                q_url.ecommerce = paste_strings(ec_list, max_length=MAX_CHARFIELD_LENGTH)
            if sm_list:
                q_url.social_media = paste_strings(sm_list, max_length=MAX_CHARFIELD_LENGTH)

            if url_analyse.exists:
                q_url.bestaat = True
                q_url.ssl = url_analyse.req.ssl
                q_url.ssl_invalid = url_analyse.req.ssl_invalid

            postcode_lijst = url_analyse.matches[POSTAL_CODE_KEY]
            kvk_lijst = url_analyse.matches[KVK_KEY]
            btw_lijst = url_analyse.matches[BTW_KEY]
            postcode_set = set([standard_postcode(pc) for pc in postcode_lijst])
            kvk_set = set([int(re.sub(r"\.", "", kvk)) for kvk in kvk_lijst])
            btw_set = set([re.sub(r"\.", "", btw) for btw in btw_lijst])

            q_url.all_kvk = paste_strings(["{:08d}".format(kvk) for kvk in list(kvk_set)],
                                          max_length=MAX_CHARFIELD_LENGTH)
            q_url.all_btw = paste_strings(list(btw_set), max_length=MAX_CHARFIELD_LENGTH)
            q_url.all_psc = paste_strings(list(postcode_set),
                                          max_length=MAX_CHARFIELD_LENGTH)

            try:
                q_url.btw_nummer = btw_lijst[0]
            except IndexError:
                pass

            if self.save:
                q_url.save()

            logger.debug(f"Check all external url ")
            for external_url in url_analyse.external_hrefs:
                if external_url is None:
                    logger.debug("A None external href was stored. Check later why, skip for now")
                    continue
                logger.debug(f"Cleaning {external_url}")
                clean_url = get_clean_url(external_url)
                qq = self.UrlNL.select().where(self.UrlNL.url == clean_url)
                if not qq.exists():
                    logger.debug(f"Adding a new entry {clean_url}")
                    self.UrlNL.create(url=clean_url, bestaat=True, referred_by=url)
                else:
                    logger.debug(f"url is already present {external_url}")

            self.logger.debug(url_analyse)

            if pbar:
                pbar.update()

        if pbar is not None:
            pbar.close()

        duration = time.time() - start
        self.logger.info(f"Done processing in {duration} seconds")
