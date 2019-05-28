import collections
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

from cbs_utils.misc import (create_logger, is_postcode, print_banner)
from cbs_utils.web_scraping import (UrlSearchStrings, BTW_REGEXP, ZIP_REGEXP, KVK_REGEXP,
                                    get_clean_url)
from kvk_url_finder import LOGGER_BASE_NAME, CACHE_DIRECTORY
from kvk_url_finder.model_variables import COUNTRY_EXTENSIONS, SORT_ORDER_HREFS
from kvk_url_finder.models import *
from kvk_url_finder.utils import (Range, check_if_url_needs_update, UrlInfo, UrlCompanyRanking,
                                  read_sql_table, paste_strings, get_string_name_from_df)

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


def clean_name(naam):
    """
    Clean the name of a company to get a better match with the url

    Parameters
    ----------
    naam: str
        Original name of the company

    Returns
    -------
    str:
        Clean name
    """
    # de naam altijd in kleine letters
    naam_small = naam.lower()

    # alles wat er uit zit als B.V. N.V., etc  wordt verwijderd
    naam_small = re.sub("\s(\w\.)+[\s]*", "", naam_small)

    # alles wat tussen haakjes staat + wat er nog achter komt verwijderen
    naam_small = re.sub("\(.*\).*$", "", naam_small)

    # alle & tekens verwijderen
    naam_small = re.sub("[&\"]", "", naam_small)

    # alle  spaties verwijderen
    naam_small = re.sub("\s+", "", naam_small)

    return naam_small


class KvKUrlParser(mp.Process):
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
                 address_input_file_name=None,
                 url_input_file_name=None,
                 kvk_selection_input_file_name=None,
                 kvk_selection_kvk_key=None,
                 kvk_selection_kvk_sub_key=None,
                 address_keys=None,
                 kvk_url_keys=None,
                 reset_database=False,
                 reset_table_df=True,
                 extend_database=False,
                 compression=None,
                 maximum_entries=None,
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
                 filter_urls: list = None,
                 filter_kvks: list = None,
                 rescan_missing_urls: bool = False,
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
        else:
            self.logger = logging.getLogger(LOGGER_BASE_NAME)
            self.logger.setLevel(console_log_level)
            self.logger.info("Set up class logger for main {}".format(__name__))

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

        self.rescan_missing_urls = rescan_missing_urls
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

        self.address_keys = address_keys
        self.kvk_url_keys = kvk_url_keys

        self.save = save
        self.older_time = older_time
        self.timezone = timezone
        self.filter_urls = filter_urls
        self.filter_kvks = filter_kvks
        self.current_time = datetime.datetime.now(pytz.timezone(self.timezone))

        self.kvk_selection_input_file_name = kvk_selection_input_file_name
        self.kvk_selection_kvk_key = kvk_selection_kvk_key
        self.kvk_selection_kvk_sub_key = kvk_selection_kvk_sub_key
        self.kvk_selection = None

        self.impose_url_for_kvk = impose_url_for_kvk

        self.force_process = force_process

        self.n_count_threshold = n_url_count_threshold

        self.url_input_file_name = url_input_file_name
        self.address_input_file_name = address_input_file_name

        self.reset_database = reset_database
        self.reset_table_df = reset_table_df
        self.extend_database = extend_database

        self.threshold_distance = threshold_distance
        self.threshold_string_match = threshold_string_match

        self.maximum_entries = maximum_entries

        self.compression = compression
        self.progressbar = progressbar
        self.showbar = progressbar
        if singlebar and i_proc > 0 or i_proc is None:
            # in case the single bar option is given, we only show the bar of the first process
            self.showbar = False

        self.kvk_range_read = Range(kvk_range_read)

        self.kvk_range_process = Range(kvk_range_process)

        self.company_df: pd.DataFrame = None
        self.url_df: pd.DataFrame = None
        self.address_df: pd.DataFrame = None
        self.kvk_df: pd.DataFrame = None
        self.website_df: pd.DataFrame = None

        self.company_vs_kvk = None
        self.n_company = None

        self.company_urls_df: pd.DataFrame = None
        self.company_address_df: pd.DataFrame = None

        self.number_of_processes = number_of_processes

        self.kvk_ranges = None

        self.database = init_database(database_name, database_type=database_type,
                                      user=user, password=password, host=hostname)
        self.database.execute_sql("SET TIME ZONE '{}'".format(self.timezone))
        tables = init_models(self.database, self.reset_database)
        self.UrlNLTbl = tables[0]
        self.CompanyTbl = tables[1]
        self.AddressTbl = tables[2]
        self.WebsiteTbl = tables[3]

    def run(self):
        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.logger.debug("Matching the best url's")
        self.find_best_matching_url()

    def generate_sql_tables(self):
        self.read_database_addresses()
        self.read_database_urls()
        self.merge_data_base_kvks()

        self.company_kvks_to_sql()
        self.url_nl_to_sql()
        self.urls_per_kvk_to_sql()
        self.addresses_per_kvk_to_sql()

    def populate_dataframes(self, only_the_company_df=False, only_found_urls=False):
        """
        Read the sql tables into pandas dataframes

        Parameters
        ----------
        only_the_company_df: bool
            If true, only read the company_df table. This is the only table we need the first round

        Notes
        -----
        * The start and stop of the range of kvk numbers kan be given here, the selection of the
          range is made in the query
        * The names of the table correspond to the names of the Table as defined in the models, but
          only with lower chars and _ at the camelcase positions (UrlNl -> url_nl)

        """
        start = self.kvk_range_process.start
        stop = self.kvk_range_process.stop
        if self.kvk_range_process.selection is not None:
            kvk_selection = self.kvk_range_process.selection
        elif self.kvk_selection is not None:
            kvk_selection = list(self.kvk_selection.values)
        else:
            kvk_selection = None

        if self.rescan_missing_urls:
            sql_command = f"select {COMPANY_ID_KEY}, count(*)-count({BESTAAT_KEY}) as missing "
            sql_command += "from web_site"
            sel = read_sql_table(table_name="web_site", connection=self.database,
                                 variable=COMPANY_ID_KEY, lower=start, upper=stop,
                                 sql_command=sql_command, group_by=COMPANY_ID_KEY)
            missing = sel[sel["missing"] > 0]
            selection = list(missing[COMPANY_ID_KEY].values)
        else:
            selection = kvk_selection

        sql_table = read_sql_table(table_name="company", connection=self.database,
                                   variable=KVK_KEY, datetime_key=DATETIME_KEY, lower=start,
                                   upper=stop, max_query=self.maximum_entries,
                                   force_process=self.force_process,
                                   older_time=self.older_time,
                                   selection=selection)
        self.company_df = sql_table
        self.company_df.set_index(KVK_KEY, inplace=True, drop=True)
        self.company_df.sort_index(inplace=True)

        # convert the timezone of the date/time stamp (which is stored in utc in sql) to our time
        # note that you need to use the dt operator before converting the date/times
        try:
            self.company_df[DATETIME_KEY] = \
                self.company_df[DATETIME_KEY].dt.tz_convert(self.timezone)
        except AttributeError:
            logger.debug("Could not convert the date times in the company table. Probably empty")

        if not only_the_company_df:

            self.address_df = read_sql_table(table_name="address", connection=self.database,
                                             variable=KVK_KEY,
                                             selection=list(self.company_df.index))
            self.website_df = read_sql_table(table_name="web_site", connection=self.database,
                                             variable=COMPANY_ID_KEY,
                                             lower=start, upper=stop,
                                             selection=list(self.company_df.index))
            self.website_df.rename(columns={COMPANY_ID_KEY: KVK_KEY, URL_ID_KEY: URL_KEY},
                                   inplace=True)

            self.website_df.loc[:, DISTANCE_STRING_MATCH_KEY] = None

            if only_found_urls:
                url_selection = list(self.website_df[URL_KEY].values)
            else:
                url_selection = None

            self.url_df = read_sql_table(table_name="url_nl", connection=self.database,
                                         variable=URL_KEY, selection=url_selection)

            self.url_df.set_index(URL_KEY, inplace=True, drop=True)
            self.url_df.sort_index(inplace=True)
            try:
                self.url_df[DATETIME_KEY] = self.url_df[DATETIME_KEY].dt.tz_convert(self.timezone)
            except AttributeError:
                logger.debug("Could not convert the date times in the url table. Probably empty")

    def get_kvk_list_per_process(self):
        """
        The company_df contains all the kvk to process. Divide them here into range per processor
        """
        # we dont have to filter the kvk range; already done with reading
        number_in_range = self.company_df.index.size
        if number_in_range == 0:
            raise ValueError(f"No kvk numbers where found in range")

        if self.filter_kvks:
            overlap_kvk = self.company_df.index.intersection(set(self.filter_kvks))
            self.company_df = self.company_df.loc[overlap_kvk]

        # set flag for all kvk processed longer than older_time ago
        delta_time = self.current_time - self.company_df[DATETIME_KEY]
        mask = (delta_time >= self.older_time) | delta_time.isna()
        if not self.force_process and not self.rescan_missing_urls:
            self.company_df = self.company_df[mask]

        n_kvk = self.company_df.index.size
        self.logger.debug(f"Found {n_kvk} kvk's to process")

        # check the ranges
        if n_kvk == 0:
            raise ValueError(f"Found {number_in_range} kvk numbers in range"
                             f"but none to be processed")

        if n_kvk < self.number_of_processes:
            raise ValueError(f"Found {number_in_range} kvk numbers in range"
                             f"with {n_kvk} to process, with only {self.number_of_processes} cores")

        n_per_proc = int(n_kvk / self.number_of_processes)
        if n_kvk % self.number_of_processes > 0:
            # make sure to add one to all processes  to take care the the left overs
            n_per_proc += 1
        self.logger.debug(f"Number of kvk's per process: {n_per_proc}")
        self.kvk_ranges = list()

        for i_proc in range(self.number_of_processes):
            if i_proc == self.number_of_processes - 1:
                kvk_proc = self.company_df.index.values[i_proc * n_per_proc:]
            else:
                kvk_proc = self.company_df.index.values[
                           i_proc * n_per_proc:(i_proc + 1) * n_per_proc]

            if self.kvk_selection_input_file_name is not None:
                # we have passed a selection file. So explicity add this selection
                kvk_selection = kvk_proc
            else:
                kvk_selection = None

            if kvk_proc.size > 0:
                logger.info("Getting range")
                kvk_first = kvk_proc[0]
                kvk_last = kvk_proc[-1]
                self.kvk_ranges.append(dict(start=kvk_first, stop=kvk_last, selection=kvk_selection))

    def merge_external_database(self):
        """
        Merge the external database

        Returns
        -------

        """
        self.logger.debug("Start merging..")

        infile = Path(self.kvk_selection_input_file_name)
        outfile_ext = infile.suffix
        outfile_base = infile.resolve().stem

        outfile = Path(outfile_base + "_merged" + outfile_ext)

        query = self.CompanyTbl.select()
        df_sql = pd.DataFrame(list(query.dicts()))
        df_sql.set_index(KVK_KEY, inplace=True)

        df = pd.read_excel(self.kvk_selection_input_file_name)

        df.rename(columns={self.kvk_selection_kvk_key: KVK_KEY}, inplace=True)

        df[KVK_KEY] = df[KVK_KEY].fillna(0).astype(int)

        df.set_index([KVK_KEY, self.kvk_selection_kvk_sub_key], inplace=True)

        result = df.merge(df_sql, left_on=KVK_KEY, right_on=KVK_KEY)

        result.reset_index(inplace=True)
        result.rename(columns={KVK_KEY: self.kvk_selection_kvk_key}, inplace=True)

        self.logger.info("Writing merged data base to {}".format(outfile.name))
        result.to_excel(outfile.name)

        self.logger.debug("Merge them")

    def read_database_selection(self):
        """
        Read the external data base that contains a selection of kvk number we want to process
        """
        self.logger.info("Reading selection data base")
        df = pd.read_excel(self.kvk_selection_input_file_name)

        df.drop_duplicates([self.kvk_selection_kvk_key], inplace=True)

        self.kvk_selection = df[self.kvk_selection_kvk_key].dropna().astype(int)

    def export_df(self, file_name):
        export_file = Path(file_name)
        if export_file.suffix in (".xls", ".xlsx"):
            self.logger.info(f"Export dataframes to {export_file}")
            with pd.ExcelWriter(file_name) as writer:
                for cnt, (naam, df) in enumerate(zip(
                        ["company_df", "address_df", "website_df", "url_df"],
                        [self.company_df, self.address_df, self.website_df, self.url_df])):
                    if df is not None:
                        self.logger.info(f"Appending sheet {naam}")
                        try:
                            df[DATETIME_KEY] = df[DATETIME_KEY].astype(str)
                        except KeyError:
                            pass
                        df.to_excel(writer, sheet_name=naam)
        elif export_file.suffix == ".csv":
            for cnt, (naam, df) in enumerate(zip(
                    ["company_df", "address_df", "website_df", "url_df"],
                    [self.company_df, self.address_df, self.website_df, self.url_df])):
                this_name = export_file.stem + "_" + naam + ".csv"
                self.logger.info(f"Writing to {naam}")
                if df is not None:
                    df.to_csv(this_name)
        else:
            self.logger.warning(f"Extension {export_file.suffix} not recognised")

    def export_db(self, file_name):

        export_file = Path(file_name)
        if export_file.suffix in (".xls", ".xlsx"):
            with pd.ExcelWriter(file_name) as writer:
                for cnt, table in enumerate([self.CompanyTbl, self.AddressTbl, self.WebsiteTbl]):
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
            for cnt, table in enumerate([self.CompanyTbl, self.AddressTbl, self.WebsiteTbl]):
                this_name = export_file.stem + "_" + table.__name__.lower() + ".csv"
                query = table.select()
                df = pd.DataFrame(list(query.dicts()))
                try:
                    df.set_index(KVK_KEY, inplace=True)
                except KeyError:
                    pass
                self.logger.info(f"Writing to {this_name}")
                df.to_csv(this_name)

    def merge_data_base_kvks(self):
        """
        Merge the data base kvks.

        The kvks in the url data base should be a subset of the url in the address data base
        """

        # create a data frame with all the unique kvk number/name combi
        df = self.url_df[[KVK_KEY, NAME_KEY]]
        df.set_index(KVK_KEY, inplace=True, drop=True)
        df = df[~df.index.duplicated()]

        # also create a data frame from the unique address kvk's
        name_key2 = NAME_KEY + "2"
        df2 = self.address_df[[KVK_KEY, NAME_KEY]]
        df2 = df2.rename(columns={NAME_KEY: name_key2})
        df2.set_index(KVK_KEY, inplace=True, drop=True)
        df2 = df2[~df2.index.duplicated()]

        # merge them on the outer, so we can create a combined kvk list
        df3 = pd.concat([df, df2], axis=1, join="outer")

        # replace al the empty field in NAME_KEY with tih
        df3[NAME_KEY].where(~df3[NAME_KEY].isnull(), df3[name_key2], inplace=True)

        df3.drop(name_key2, inplace=True, axis=1)

        difference = df3.index.difference(df2.index)
        new_kvk_name = df3.loc[difference, :]

        n_before = self.address_df.index.size
        self.address_df.set_index(KVK_KEY, inplace=True)

        # append the new address to the address data base
        self.address_df = pd.concat([self.address_df, new_kvk_name], axis=0, sort=True)
        self.address_df.sort_index(inplace=True)
        self.address_df.reset_index(inplace=True)
        try:
            self.address_df.drop(["index"], axis=1, inplace=True)
        except KeyError as err:
            self.logger.info(err)

        n_after = self.address_df.index.size
        self.logger.info("Added {} kvk from url list to addresses".format(n_after - n_before))

    # @profile
    def find_best_matching_url(self):
        """
        Per company, see which url matches the best the company based on the name + the web info
        """

        if self.filter_kvks:
            overlap_kvk = self.company_df.index.intersection(set(self.filter_kvks))
            self.company_df = self.company_df.loc[overlap_kvk]

        # set flag for all kvk processed longer than older_time ago
        delta_time = self.current_time - self.company_df[DATETIME_KEY]
        mask = (delta_time >= self.older_time) | delta_time.isna()
        if not self.force_process and not self.rescan_missing_urls:
            self.company_df = self.company_df[mask.values]

        self.logger.info("Start finding best matching urls for proc {}".format(self.i_proc))

        # count the number of none-processed queries (ie in which the processed flag == False
        # we have already imposed the max_entries option in the selection of the ranges
        self.logger.info("Counting all...")
        if self.maximum_entries:
            max_queries = self.maximum_entries
        else:
            max_queries = self.company_df.index.size
        self.logger.info("Start processing {} queries".format(max_queries))

        if self.progressbar and self.showbar:
            pbar = tqdm(total=max_queries, position=self.i_proc, file=sys.stdout)
            pbar.set_description("@{:2d}: ".format(self.i_proc))
        else:
            pbar = None

        start = time.time()
        # loop over all the companies kvk numbers
        cnt = 0
        for index, row in self.company_df.iterrows():

            # first check if we do not have to stop
            if self.maximum_entries is not None and cnt == self.maximum_entries:
                self.logger.info("Maximum entries reached")
                break
            if os.path.exists(STOP_FILE):
                self.logger.info("Stop file found. Quit processing")
                os.remove(STOP_FILE)
                break

            kvk_nummer = index
            company_name = get_string_name_from_df(NAME_KEY, row, index, self.company_df)
            self.logger.info("Processing {} ({})".format(kvk_nummer, company_name))

            cnt += 1

            if self.search_urls:
                self.logger.info("Start a URL search for this company first")

            # for this kvk, get the list of urls + the address info
            company_urls_df = self.website_df[self.website_df[KVK_KEY] == kvk_nummer].reset_index()
            company_addresses_df = self.address_df[self.address_df[KVK_KEY] == kvk_nummer]

            try:
                # match the url with the name of the company
                company_url_match = \
                    CompanyUrlMatch(company_record=row,
                                    kvk_nr=kvk_nummer,
                                    company_name=company_name,
                                    current_time=self.current_time,
                                    company_urls_df=company_urls_df,
                                    company_addresses_df=company_addresses_df,
                                    urls_df=self.url_df,
                                    imposed_urls=self.impose_url_for_kvk,
                                    distance_threshold=self.threshold_distance,
                                    string_match_threshold=self.threshold_string_match,
                                    i_proc=self.i_proc,
                                    store_html_to_cache=self.store_html_to_cache,
                                    max_cache_dir_size=self.max_cache_dir_size,
                                    internet_scraping=self.internet_scraping,
                                    older_time=self.older_time,
                                    timezone=self.timezone,
                                    exclude_extension=self.exclude_extension,
                                    filter_urls=self.filter_urls,
                                    force_process=self.force_process,
                                    rescan_missing_urls=self.rescan_missing_urls,
                                    logger=self.logger
                                    )

                self.logger.debug("Done with {}".format(company_url_match.company_name))
            except pw.DatabaseError as err:
                self.logger.warning(f"{err}")
                self.logger.warning("skipping")
            else:
                # succeeded the match. Now update the sql tables atomic
                self.update_sql_tables(kvk_nummer, company_url_match)

            if pbar:
                pbar.update()

        if pbar is not None:
            pbar.close()

        duration = time.time() - start
        self.logger.info(f"Done processing in {duration} seconds")
        # this is not faster than save per record
        # with Timer("Updating tables") as _:
        #    query = (Company.update(dict(url=Company.url, processed=Company.processed)))
        #    query.execute()

    def update_for_url_info_without_match(self, kvk_nummer, url_info: UrlInfo = None,
                                          update_company_table: bool = True):

        if url_info is not None:
            url = url_info.url
            url_analyse = url_info.url_analyse
            logger.debug(f"No match info found  {kvk_nummer} for {url}, but we have urls")
        else:
            url = None
            url_analyse = None
            logger.debug(f"No match info found  {kvk_nummer} because we do not have urls")

        if url_analyse is not None:
            datetime = url_analyse.process_time
            bestaat = url_analyse.exists
        else:
            datetime = self.current_time
            bestaat = None

        if url is not None:
            query = self.WebsiteTbl.update(
                url=url,
                getest=True,
                bestaat=bestaat,
                levenshtein=None,
                string_match=None,
                url_match=None,
                url_rank=None,
                has_postcode=None,
                has_kvk_nr=None,
                has_btw_nr=None,
                ranking=None,
                nl_company=False,
                best_match=False,
                datetime=datetime
            ).where(self.WebsiteTbl.company_id == kvk_nummer and self.WebsiteTbl.url_id == url)
            query.execute()

            query = self.UrlNLTbl.select().where(self.UrlNLTbl.url == url)
            if query.exists():
                logger.debug(f"Updating UrlNl {url}")
                query = self.UrlNLTbl.update(
                    bestaat=bestaat,
                    nl_company=False,
                    datetime=datetime
                ).where(self.UrlNLTbl.url == url)
                query.execute()

        if update_company_table:
            logger.debug(f"Updating CompanyTbl {kvk_nummer} for {url}")
            query = self.CompanyTbl.update(
                core_id=self.i_proc,
                datetime=datetime
            ).where(self.CompanyTbl.kvk_nummer == kvk_nummer)
            query.execute()

    def update_for_url_info_with_match(self, kvk_nummer, url_info, url):
        """
        Update the sql tables in case we have at least one matching url found for the company

        Parameters
        ----------
        kvk_nummer: int
            Kvk number of the current company we are processing
        url_info: object UrlInfo
            Class holding the links to both the url_analyse and match result
        """
        # url = url_info.url

        logger.debug(f"No match info found  {kvk_nummer} for {url}")

        url_analyse = url_info.url_analyse
        match = url_info.match

        try:
            ranking_int = int(round(match.ranking))
        except AttributeError:
            ranking_int = None

        if match.best_match:
            logger.debug(f"Updating CompanyTbl {kvk_nummer} for {url}")
            query = self.CompanyTbl.update(
                url=url,
                ranking=ranking_int,
                core_id=self.i_proc,
                datetime=url_analyse.process_time
            ).where(self.CompanyTbl.kvk_nummer == kvk_nummer)
            query.execute()

        logger.debug(f"Updating WebsiteTbl {url}")
        query = self.WebsiteTbl.update(
            url=url,
            getest=True,
            bestaat=url_analyse.exists,
            levenshtein=match.distance,
            string_match=match.string_match,
            url_match=match.url_match,
            url_rank=match.url_rank,
            best_match=match.best_match,
            has_postcode=match.has_postcode,
            has_kvk_nr=match.has_kvk_nummer,
            has_btw_nr=match.has_btw_nummer,
            ranking=match.ranking,
            datetime=url_analyse.process_time
        ).where(self.WebsiteTbl.company_id == kvk_nummer and self.WebsiteTbl.url_id == url)
        query.execute()

        try:
            all_psc = paste_strings(list(match.postcode_set), max_length=MAX_CHARFIELD_LENGTH)
        except TypeError:
            all_psc = None
        try:
            all_kvk = paste_strings(list(match.kvk_set), max_length=MAX_CHARFIELD_LENGTH)
        except TypeError:
            all_kvk = None
        ecommerce = paste_strings(url_info.ecommerce, max_length=MAX_CHARFIELD_LENGTH)
        social_media = paste_strings(url_info.social_media, max_length=MAX_CHARFIELD_LENGTH)
        if url_analyse.req is not None:
            ssl = url_analyse.req.ssl
            ssl_valid = url_analyse.req.ssl_valid
        else:
            ssl = None
            ssl_valid = None

        query = self.UrlNLTbl.select().where(self.UrlNLTbl.url == url)
        if query.exists():
            logger.debug(f"Updating UrlNl {url}")
            query = self.UrlNLTbl.update(
                bestaat=url_analyse.exists,
                nl_company=match.nl_company,
                post_code=match.matched_postcode,
                kvk_nummer=match.matched_kvk_nummer,
                btw_nummer=match.btw_nummer,
                datetime=url_analyse.process_time,
                ssl=ssl,
                ssl_valid=ssl_valid,
                subdomain=match.ext.subdomain,
                domain=match.ext.domain,
                suffix=match.ext.suffix,
                category=url_info.category,
                ecommerce=ecommerce,
                social_media=social_media,
                all_psc=all_psc,
                all_kvk=all_kvk,
                all_btw=match.btw_nummer
            ).where(self.UrlNLTbl.url == url)
            query.execute()

        # loop over the external links in the url
        if url_info.url_analyse:
            for ext_url in url_info.url_analyse.external_hrefs:
                clean_url = get_clean_url(ext_url)
                query = self.UrlNLTbl.select().where(self.UrlNLTbl.url == clean_url)
                if not query.exists():
                    logger.debug(f"Adding a new entry {clean_url}")
                    self.UrlNLTbl.create(url=clean_url, bestaat=True, referred_by=url)
                else:
                    logger.debug(f"External already  {clean_url}")

    def update_sql_tables(self, kvk_nummer, company_url_match):
        """
        Transfer the match data from the data frame to the sql tabels
        """

        logger.info(f"Updating TABLES for {kvk_nummer}")
        update_company = True
        if not company_url_match.urls.collection:
            self.update_for_url_info_without_match(kvk_nummer)
        else:
            for url, url_info in company_url_match.urls.collection.items():
                logger.debug(f"storing {url}")

                if url_info.match is None:
                    self.update_for_url_info_without_match(kvk_nummer,
                                                           url_info=url_info,
                                                           update_company_table=update_company)
                    update_company = False
                else:
                    self.update_for_url_info_with_match(kvk_nummer, url_info, url)

    # @profile
    def read_csv_input_file(self,
                            file_name: str,
                            usecols: list = None,
                            names: list = None,
                            remove_spurious_urls=False,
                            unique_key=None
                            ):
        """
        Store the csv file in a data frame

        Parameters
        ----------
        file_name: str
            File name to read from
        usecols: list
            Selection of columns to read
        names: list
            Names to give to the columns
        remove_spurious_urls: bool
            If true we are reading the urls, so we can remove all the urls that occur many times
        unique_key: str
            We can select a range of kvk number if we know the unique key to make groups or kvks

        Returns
        -------
        DataFrame:
            Dataframe with the data

        """

        # split the extension two time so we can also deal with a double extension bla.csv.zip
        file_base, file_ext = os.path.splitext(file_name)
        file_base2, file_ext2 = os.path.splitext(file_base)

        # build the cache file including the cache_directory
        cache_file = Path(CACHE_DIRECTORY) / (file_base2 + ".pkl")

        if os.path.exists(cache_file):
            # add the type so we can recognise it is a data frame
            self.logger.info("Reading from cache {}".format(cache_file))
            df: pd.DataFrame = pd.read_pickle(cache_file)
            df.reset_index(inplace=True)
        elif ".csv" in (file_ext, file_ext2):
            self.logger.info("Reading from file {}".format(file_name))
            df = pd.read_csv(file_name,
                             header=None,
                             usecols=usecols,
                             names=names
                             )

            if remove_spurious_urls:
                self.logger.info("Removing spurious urls")
                df = self.remove_spurious_urls(df)

            df = self.clip_kvk_range(df, unique_key=unique_key, kvk_range=self.kvk_range_read)

            self.logger.info("Writing data to cache {}".format(cache_file))
            df.to_pickle(cache_file)
        else:
            raise AssertionError("Can only read h5 or csv files")

        try:
            df.drop("index", axis=0, inplace=True)
        except KeyError:
            self.logger.debug("No index to drop")
        else:
            self.logger.debug("Dropped index")

        return df

    # @profile
    def read_database_urls(self):
        """
        Read the URL data from the csv file or hd5 file
        """

        col_kvk = self.kvk_url_keys[KVK_KEY]
        col_name = self.kvk_url_keys[NAME_KEY]
        col_url = self.kvk_url_keys[URL_KEY]

        self.url_df = self.read_csv_input_file(self.url_input_file_name,
                                               usecols=[col_kvk, col_name, col_url],
                                               names=[KVK_KEY, NAME_KEY, URL_KEY],
                                               remove_spurious_urls=True,
                                               unique_key=URL_KEY)

        self.logger.info("Removing duplicated table entries")
        self.remove_duplicated_url_entries()

    # @profile
    def clip_kvk_range(self, dataframe, unique_key, kvk_range):
        """
        Make a selection of kvk numbers

        Returns
        -------

        """

        start = kvk_range.start
        stop = kvk_range.stop
        n_before = dataframe.index.size

        if start is not None or stop is not None or self.kvk_selection_kvk_key is not None:
            self.logger.info("Selecting kvk number from {} to {}".format(start, stop))
            idx = pd.IndexSlice
            df = dataframe.set_index([KVK_KEY, unique_key])

            if self.kvk_selection_kvk_key is not None:
                df = df.loc[idx[self.kvk_selection, :], :]

            if start is None:
                df = df.loc[idx[:stop, :], :]
            elif stop is None:
                df = df.loc[idx[start:, :], :]
            else:
                df = df.loc[idx[start:stop, :], :]
            df.reset_index(inplace=True)
        else:
            df = dataframe

        n_after = df.index.size
        self.logger.debug("Kept {} out of {} records".format(n_after, n_before))

        # check if we have  any valid entries in the range
        if n_after == 0:
            self.logger.info(dataframe.info())
            raise ValueError("No records found in kvk range {} {} (kvk range: {} -- {})".format(
                start, stop, dataframe[KVK_KEY].min(), dataframe[KVK_KEY].max()))

        return df

    # @profile
    def read_database_addresses(self):
        """
        Read the URL data from the csv file or hd5 file
        """

        col_kvk = self.address_keys[KVK_KEY]
        col_name = self.address_keys[NAME_KEY]
        col_adr = self.address_keys[ADDRESS_KEY]
        col_post = self.address_keys[POSTAL_CODE_KEY]
        col_city = self.address_keys[CITY_KEY]

        self.address_df = self.read_csv_input_file(self.address_input_file_name,
                                                   usecols=[col_kvk, col_name, col_adr,
                                                            col_post, col_city],
                                                   names=[KVK_KEY, NAME_KEY, ADDRESS_KEY,
                                                          POSTAL_CODE_KEY, CITY_KEY],
                                                   unique_key=POSTAL_CODE_KEY)
        self.remove_duplicated_kvk_entries()

        self.logger.debug("Done")

    # @profile
    def look_up_last_entry(self, n_skip_entries):
        """
        Get the last entry in the data base

        Parameters
        ----------
        n_skip_entries: int
            Number of entries in csv file to skip based on the total amount of entries in the
            current database sql file

        Notes
        -----
        In case we have N entries in the data base we want to continue reading in the csv file
        after 'at' least N entries. However, N could be larger, because we have removed url's before
        we wrote to the data base. This means that we can increase n. This is taken care of here
        """
        # get the last kvk number of the website list
        last_website = self.WebsiteTbl.select().order_by(self.WebsiteTbl.company_id.desc()).get()
        kvk_last = int(last_website.company.kvk_nummer)

        try:
            # based on the last kvk in the database, get the index in the csv file
            # note that with the loc selection we get all URL's belongin to this kvk. Therfore
            # take the last of this list with -1
            row_index = self.url_df.loc[self.url_df[KVK_KEY] == kvk_last].index[-1]
        except IndexError:
            self.logger.debug(
                "No last index found.  n_entries to skip to {}".format(n_skip_entries))
        else:
            # we have the last row index. This means that we can add this index to the n_entries
            # we have used now. Return this n_entries
            last_row = self.url_df.loc[row_index]
            self.logger.debug("found: {}".format(last_row))
            n_skip_entries += row_index + 1
            self.logger.debug("Updated n_entries to skip to {}".format(n_skip_entries))

        return n_skip_entries

    # @profile
    def remove_spurious_urls(self, dataframe):
        # first remove all the urls that occur more the 'n_count_threshold' times.
        urls = dataframe
        # this line add the number of occurrences to each url
        #
        n_count = urls.groupby(URL_KEY)[URL_KEY].transform("count")
        url_before = set(urls[URL_KEY].values)
        urls = urls[n_count < self.n_count_threshold]
        url_after = set(urls[URL_KEY].values)
        url_removed = url_before.difference(url_after)
        self.logger.debug("Removed URLS:\n{}".format(url_removed))

        # turn the kvknumber/url combination into the index and remove the duplicates. This
        # means that per company each url only occurs one time
        urls = urls.set_index([KVK_KEY, URL_KEY]).sort_index()
        # this removes all the duplicated indices, i.e. combination kvk_number/url. So if one
        # kvk company has multiple times www.facebook.com at the web site, only is kept.
        urls = urls[~urls.index.duplicated()]

        urls.reset_index(inplace=True)

        return urls

    def remove_duplicated_kvk_entries(self):
        """
        Remove all the kvk's that have been read before and are stored in the sql already
        """

        nr = self.address_df.index.size
        self.logger.info("Removing duplicated kvk entries")
        query = self.CompanyTbl.select()
        kvk_list = list()
        try:
            for company in query:
                kvk_nummer = int(company.kvk_nummer)
                if kvk_nummer in self.address_df[KVK_KEY].values:
                    kvk_list.append(company.kvk_nummer)
        except pw.OperationalError:
            # nothing to remove
            return

        kvk_in_db = pd.DataFrame(data=kvk_list, columns=[KVK_KEY])

        kvk_in_db.set_index(KVK_KEY, inplace=True)
        kvk_to_remove = self.address_df.set_index(KVK_KEY)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove.index.duplicated()]
        kvk_to_remove = kvk_to_remove.reindex(kvk_in_db.index)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove[NAME_KEY].isnull()]
        try:
            self.address_df = self.address_df.set_index(KVK_KEY).drop(index=kvk_to_remove.index)
        except KeyError:
            self.logger.debug("Nothing to drop")
        else:
            self.address_df.reset_index(inplace=True)

    # @profile
    def remove_duplicated_url_entries(self):
        """
        Remove all the companies/url combination which already have been stored in
        the sql tables

        """

        # based on the data in the WebSite table create a data frame with all the kvk which
        # we have already included. These can be removed from the data we have just read
        nr = self.url_df.index.size
        self.logger.info("Removing duplicated kvk/url combinies. Data read at start: {}".format(nr))
        self.logger.debug("Getting all sql websides from database")
        kvk_list = list()
        url_list = list()
        name_list = list()
        query = (self.CompanyTbl
                 .select()
                 .prefetch(self.WebsiteTbl)
                 )
        for cnt, company in enumerate(query):
            kvk_nr = company.kvk_nummer
            naam = company.naam
            for web in company.websites:
                kvk_list.append(kvk_nr)
                url_list.append(web.url)
                name_list.append(naam)

        kvk_in_db = pd.DataFrame(
            data=list(zip(kvk_list, url_list, name_list)),
            columns=[KVK_KEY, URL_KEY, NAME_KEY])
        kvk_in_db.set_index([KVK_KEY, URL_KEY], drop=True, inplace=True)

        # drop all the kvk number which we already have loaded in the database
        self.logger.debug("Dropping all duplicated web sides")
        kvk_to_remove = self.url_df.set_index([KVK_KEY, URL_KEY])
        kvk_to_remove = kvk_to_remove.reindex(kvk_in_db.index)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove[NAME_KEY].isnull()]
        try:
            self.url_df = self.url_df.set_index([KVK_KEY, URL_KEY]).drop(index=kvk_to_remove.index)
        except KeyError:
            self.logger.debug("Nothing to drop")
        else:
            self.url_df.reset_index(inplace=True)

        self.logger.debug("Getting all  companies in Company table")
        kvk_list = list()
        name_list = list()
        for company in self.CompanyTbl.select():
            kvk_list.append(int(company.kvk_nummer))
            name_list.append(company.naam)
        companies_in_db = pd.DataFrame(data=list(zip(kvk_list, name_list)),
                                       columns=[KVK_KEY, NAME_KEY])
        companies_in_db.set_index([KVK_KEY], drop=True, inplace=True)

        self.logger.debug("Dropping all  duplicated companies")
        comp_df = self.url_df.set_index([KVK_KEY, URL_KEY])
        comp_df.drop(index=companies_in_db.index, level=0, inplace=True)
        self.url_df = comp_df.reset_index()

        nr = self.url_df.index.size
        self.logger.debug("Removed duplicated kvk/url combies. Data at end: {}".format(nr))

    # @profile
    def company_kvks_to_sql(self):
        """
        Write all the company kvk with name to the sql
        """
        self.logger.info("Start writing to mysql data base")
        if self.address_df.index.size == 0:
            self.logger.debug("Empty addresses data frame. Nothing to write")
            return

        self.kvk_df = self.address_df[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        # self.kvk_df.loc[:, URLNL_KEY] = None

        record_list = list(self.kvk_df.to_dict(orient="index").values())
        self.logger.info("Start writing table urls")

        n_batch = int(len(record_list) / MAX_SQL_CHUNK) + 1
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_batch)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None

        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                self.logger.info("Company chunk nr {}/{}".format(cnt + 1, n_batch))
                self.CompanyTbl.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
                    sys.stdout.flush()

        if progress:
            progress.finish()
        self.logger.debug("Done with company table")

    def url_nl_to_sql(self):
        """
        Write all the unique urls to the sql
        """
        self.logger.info("Start writing UrlNL to mysql data base")
        if self.url_df.index.size == 0:
            self.logger.debug("Empty urls data frame. Nothing to write")
            return

        urls: pd.DataFrame = self.url_df[[URL_KEY, KVK_KEY]].copy()
        urls.loc[:, KVK_KEY] = None

        urls.sort_values([URL_KEY, KVK_KEY], inplace=True)

        urls.drop_duplicates([URL_KEY], inplace=True)

        urls.set_index(URL_KEY, inplace=True, drop=True)

        urls_in_db = [q.url for q in self.UrlNLTbl.select()]

        urls.drop(urls_in_db, inplace=True)
        urls.reset_index(inplace=True)

        self.logger.info("Convert url dataframe to to list of dicts. This may take some time... ")
        record_list = list(urls.to_dict(orient="index").values())
        self.logger.info("Start writing table urls")

        n_batch = int(len(record_list) / MAX_SQL_CHUNK) + 1
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_batch)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None

        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                self.logger.info("UrlNL chunk nr {}/{}".format(cnt + 1, n_batch))
                self.UrlNLTbl.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
                    sys.stdout.flush()

        if progress:
            progress.finish()
        self.logger.debug("Done with company table")

    # @profile

    # @profile
    def urls_per_kvk_to_sql(self):
        """
        Write all URL per kvk to the WebSite Table in sql
        """
        self.logger.debug("Start writing the url to sql")

        if self.url_df.index.size == 0:
            self.logger.debug("Empty url data  from. Nothing to add to the sql database")
            return

        # create selection of data columns
        urls = self.url_df[[KVK_KEY, URL_KEY, NAME_KEY]].sort_values([KVK_KEY])
        # count the number of urls per kvk
        n_url_per_kvk = urls.groupby(KVK_KEY)[KVK_KEY].count()

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        kvk_list = self.address_df[KVK_KEY].tolist()
        self.company_vs_kvk = self.CompanyTbl.select().order_by(self.CompanyTbl.kvk_nummer)
        self.n_company = self.company_vs_kvk.count()

        kvk_comp_list = list()
        for company in self.company_vs_kvk:
            kvk_comp_list.append(int(company.kvk_nummer))
        kvk_comp = set(kvk_comp_list)
        kvk_not_in_addresses = set(kvk_list).difference(kvk_comp)

        # in case there is one kvk not in the address data base, something is wrong,
        # as we took care of that in the merge_database routine
        assert not kvk_not_in_addresses

        self.logger.info(f"Found: {self.n_company} companies")
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, self.n_company)
            progress = pb.ProgressBar(widgets=wdg, maxval=self.n_company, fd=sys.stdout).start()
        else:
            progress = None

        company_list = list()
        for counter, company in enumerate(self.company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            # we need to check if this kvk is in de address list  still
            if kvk_nr in kvk_not_in_addresses:
                self.logger.debug(f"Skipping kvk {kvk_nr} as it is not in the addresses")
                continue

            try:
                n_url = n_url_per_kvk.loc[kvk_nr]
            except KeyError:
                continue

            # add the company number of url time to the list
            company_list.extend([company] * n_url)

            if progress:
                wdg[-1] = progress_bar_message(counter, self.n_company, kvk_nr, company.naam)
                progress.update(counter)
            if counter % MAX_SQL_CHUNK == 0:
                self.logger.info(" Added {} / {}".format(counter, self.n_company))
        if progress:
            progress.finish()

        urls[COMPANY_KEY] = company_list

        # in case there is a None at a row, remove it (as there is not company found)
        urls.dropna(axis=0, inplace=True)

        # the kvk key is already visible via the company_id
        urls.drop([KVK_KEY], inplace=True, axis=1)

        self.logger.info("Converting urls to dict. This make take some time...")
        url_list = list(urls.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        self.logger.info("Start writing table urls")
        n_batch = int(len(url_list) / MAX_SQL_CHUNK) + 1
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_batch)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None
        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(url_list, MAX_SQL_CHUNK)):
                self.logger.info("URL chunk nr {}/{}".format(cnt + 1, n_batch))
                self.WebsiteTbl.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
        if progress:
            progress.finish()

        self.logger.debug("Done")

    # @profile
    def addresses_per_kvk_to_sql(self):
        """
        Write all address per kvk to the Addresses Table in sql
        """

        self.logger.info("Start writing the addressees to the sql Addresses table")
        if self.address_df.index.size == 0:
            self.logger.debug("Empty address data frame. Nothing to write")
            return

        # create selection of data columns
        self.address_df[COMPANY_KEY] = None
        columns = [KVK_KEY, NAME_KEY, ADDRESS_KEY, POSTAL_CODE_KEY, CITY_KEY, COMPANY_KEY]
        df = self.address_df[columns].copy()
        df.sort_values([KVK_KEY], inplace=True)
        # count the number of urls per kvk

        n_postcode_per_kvk = df.groupby(KVK_KEY)[KVK_KEY].count()
        self.logger.info(f"Found: {self.n_company} companies")
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, self.n_company)
            progress = pb.ProgressBar(widgets=wdg, maxval=self.n_company, fd=sys.stdout).start()
        else:
            progress = None

        company_list = list()
        for counter, company in enumerate(self.company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            # we need to check if this kvk is in de address list  still

            try:
                n_postcode = n_postcode_per_kvk.loc[kvk_nr]
            except KeyError:
                continue

            # add the company number of url time to the list
            company_list.extend([company] * n_postcode)

            if progress:
                wdg[-1] = progress_bar_message(counter, self.n_company, kvk_nr, company.naam)
                progress.update(counter)
            if counter % MAX_SQL_CHUNK == 0:
                self.logger.info(" Added {} / {}".format(counter, self.n_company))
        if progress:
            progress.finish()

        df[COMPANY_KEY] = company_list

        self.logger.info("Converting addresses to dict. This make take some time...")
        address_list = list(df.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        self.logger.info("Start writing table addresses")
        n_batch = int(len(address_list) / MAX_SQL_CHUNK) + 1
        if self.progressbar:
            wdg = PB_WIDGETS
            wdg[-1] = progress_bar_message(0, self.n_company)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None
        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(address_list, MAX_SQL_CHUNK)):
                self.logger.info("Address chunk nr {}/{}".format(cnt + 1, n_batch))
                self.AddressTbl.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
        if progress:
            progress.finish()


class CompanyUrlMatch(object):
    """
    Take the company record as input and find the best matching url
    """

    def __init__(self,
                 company_record,
                 kvk_nr: int = None,
                 company_name: str = None,
                 current_time: datetime.datetime = None,
                 company_urls_df: pd.DataFrame = None,
                 company_addresses_df: pd.DataFrame = None,
                 urls_df: pd.DataFrame = None,
                 imposed_urls: dict = None,
                 distance_threshold: int = 10,
                 string_match_threshold: float = 0.5,
                 save: bool = True,
                 i_proc=0,
                 store_html_to_cache: bool = False,
                 max_cache_dir_size: int = None,
                 internet_scraping: bool = True,
                 older_time: datetime.timedelta = None,
                 timezone=None,
                 exclude_extension=None,
                 filter_urls: list = None,
                 force_process: bool = False,
                 rescan_missing_urls: bool = False,
                 logger: logging.Logger = None
                 ):
        if logger is None:
            self.logger = logging.getLogger(LOGGER_BASE_NAME)
        else:
            self.logger = logger
        self.logger.debug("Company match in debug mode")
        self.save = save
        self.i_proc = i_proc
        self.older_time = older_time
        self.timezone = timezone
        self.filter_urls = filter_urls
        self.current_time = current_time
        self.force_process = force_process
        self.rescan_missing_urls = rescan_missing_urls

        self.kvk_nr = kvk_nr
        self.company_name = company_name

        self.company_record = company_record

        self.company_urls_df = company_urls_df

        # create a database holding the urls for this company + all the info

        # the impose_url_for_kvk dictionary gives all the kvk numbers for which we just want to
        # impose a url
        if imposed_urls is not None:
            impose_url = imposed_urls.get(self.kvk_nr)
        else:
            impose_url = None

        print_banner(f"Matching Company {self.company_name}", top_symbol="+")

        # first collect all the urls and obtain the match properties
        self.logger.debug("Get Url collection....")
        self.collection = UrlCollection(company_record, self.company_name, self.kvk_nr,
                                        current_time=self.current_time,
                                        company_urls_df=self.company_urls_df,
                                        company_addresses_df=company_addresses_df, urls_df=urls_df,
                                        threshold_distance=distance_threshold,
                                        threshold_string_match=string_match_threshold,
                                        impose_url=impose_url,
                                        store_html_to_cache=store_html_to_cache,
                                        max_cache_dir_size=max_cache_dir_size,
                                        internet_scraping=internet_scraping,
                                        older_time=self.older_time, timezone=self.timezone,
                                        exclude_extensions=exclude_extension,
                                        filter_urls=self.filter_urls,
                                        force_process=self.force_process,
                                        rescan_missing_urls=self.rescan_missing_urls,
                                        logger=self.logger)
        self.urls = self.collection

        # make a copy link of the company_urls_df from the urls object to here
        self.find_match_for_company()

    def find_match_for_company(self):
        """
        Get the best matching url based on the already calculated levensteihn distance and string
        match
        """
        if self.urls.company_urls_df is not None:
            # the first row in the data frame is the best matching web site
            best_match = self.urls.company_urls_df.head(1)

            # store the best matching web site
            web_match_index = best_match.index.values[0]

            # store in the df itself
            self.urls.company_urls_df.loc[:, BEST_MATCH_KEY] = False
            self.urls.company_urls_df.loc[web_match_index, BEST_MATCH_KEY] = True

            # also store the best match in the collection
            url_best = self.urls.company_urls_df.loc[web_match_index, URL_KEY]
            for url, url_info in self.urls.collection.items():
                if url_info is None or url_info.match is None:
                    continue
                if url == url_best:
                    url_info.match.best_match = True
                else:
                    url_info.match.best_match = False

            self.logger.debug("Best matching url: {}".format(best_match.url))


class UrlCollection(object):
    """
    Analyses all potential urls of one single company. Each url is ranked based on how close it
    matches with the company based on the following score:

    has postcode: 1 pt
    has kvknummer: 3 pt
    has btwnummer: 5 pt
    levenstein distance  < 10: 1
    string match  > 0.5: 1
    """

    def __init__(self,
                 company_record,
                 company_name: str,
                 kvk_nr: int,
                 current_time=None,
                 company_urls_df: pd.DataFrame = None,
                 company_addresses_df: pd.DataFrame = None,
                 urls_df: pd.DataFrame = None,
                 threshold_distance: int = 10,
                 threshold_string_match: float = 0.5,
                 impose_url: str = None,
                 scraper="bs4",
                 store_html_to_cache=False,
                 max_cache_dir_size=None,
                 internet_scraping: bool = True,
                 older_time: datetime.timedelta = None,
                 timezone: pytz.timezone = None,
                 exclude_extensions: pd.DataFrame = None,
                 filter_urls: list = None,
                 force_process: bool = False,
                 rescan_missing_urls: bool = False,
                 logger: logging.Logger = None
                 ):
        if logger is None:
            self.logger = logging.getLogger(LOGGER_BASE_NAME)
        else:
            self.logger = logger
        self.logger.debug("Collect urls {}".format(company_name))

        self.older_time = older_time
        self.force_process = force_process
        self.rescan_missing_urls = rescan_missing_urls
        self.timezone = timezone
        self.exclude_extensions = exclude_extensions
        self.filter_urls = filter_urls
        self.current_time = current_time

        assert scraper in SCRAPERS

        self.store_html_to_cache = store_html_to_cache
        self.max_cache_dir_size = max_cache_dir_size
        self.internet_scraping = internet_scraping

        self.kvk_nr = kvk_nr
        self.company_record = company_record
        self.company_name = company_name
        self.company_urls_df = company_urls_df
        self.company_addresses_df = company_addresses_df
        self.company_name_small = clean_name(self.company_name)
        self.urls_df = urls_df

        self.postcodes = list()
        postcodes = self.company_addresses_df[POSTAL_CODE_KEY]
        is_valid_post_code = list(map(is_postcode, postcodes))
        postcodes = postcodes[is_valid_post_code]
        if postcodes.size > 0:
            self.postcodes = set(postcodes)
        else:
            self.postcodes = set()

        self.threshold_distance = threshold_distance
        self.threshold_string_match = threshold_string_match

        # remove space and put to lower for better comparison with the url
        self.logger.debug(
            "Checking {}: {} ({})".format(self.kvk_nr, self.company_name,
                                          self.company_name_small))
        self.collection = None
        self.collect_web_sites()

        self.logger.debug("Get best match")
        if impose_url:
            # just select the url to impose
            self.logger.info(f"Imposing {impose_url}")
            mask = self.company_urls_df[URL_KEY] == impose_url
            self.company_urls_df = self.company_urls_df[mask].copy()
        elif self.company_urls_df is not None:
            self.get_best_matching_web_site()
            self.logger.debug("Best Match".format(self.company_urls_df.head(1)))
        else:
            self.logger.info("No website found for".format(self.company_name))

    def get_url_nl_query(self, url):
        """
        Check the url to see if it needs updated or not based on the last processing time

        Parameters
        ----------
        url: str
            url str to update

        Returns
        -------
        url_nl or None:
            query with the url_nl update

        Notes
        -----
        If the query needs to be updated, store the current processing time and set the update flag
        to true, otherwise the flag to update is false (we skip this query) and nothing is updated
        """
        # check if we can get the url from the UrlNL table
        try:
            url_nl = self.UrlNlTbl[url]
        except (self.UrlNlTbl.DoesNotExist, IndexError) as err:
            logger.warning("not found in UrlNL in table {}. Skipping with err:{} ".format(url, err))
            url_nl = None
        else:
            logger.debug("found in UrlNL in table {} ".format(url_nl.url))

        return url_nl

    def scrape_url_and_store_in_dataframes(self, url, url_info):
        """
        Start scraping the url and store some info in the tables web and url_df
        Parameters
        ----------
        url: str
            Name of the url
        url_info: 'object':UrlInfo
            Class with information on the current url

        Returns
        -------
        object:
            :class:UrlSearchString
        """

        self.logger.debug("Start Url Search : {}".format(url))

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
                                       scrape_url=url_info.needs_update
                                       )

        # TODO: transfer this outside
        self.logger.debug("Done with URl Search: {}".format(url_analyse.matches))

        if not url_info.needs_update:
            logger.debug("We skipped the scraping so get the previous data ")
            url_analyse.exists = True
            # we have not scraped the url, but we want to set the info anyways
            postcodes = self.urls_df.loc[url, ALL_PSC_KEY]
            if postcodes is not None and postcodes != "":
                url_analyse.matches[POSTAL_CODE_KEY] = postcodes.split(",")
            btw_nummers = self.urls_df.loc[url, ALL_BTW_KEY]
            if btw_nummers is not None and btw_nummers != "":
                url_analyse.matches[BTW_KEY] = btw_nummers.split(",")
            kvk_nummers = self.urls_df.loc[url, ALL_KVK_KEY]
            if kvk_nummers is not None and kvk_nummers != "":
                url_analyse.matches[KVK_KEY] = kvk_nummers.split(",")

            e_commerce = self.urls_df.loc[url, ECOMMERCE_KEY]
            if e_commerce is not None and e_commerce != "":
                url_info.ecommerce = e_commerce.split(",")

            social_media = self.urls_df.loc[url, SOCIAL_MEDIA_KEY]
            if social_media is not None and social_media != "":
                url_info.social_media = social_media.split(",")
        else:
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
            url_info.ecommerce = ec_list
            url_info.social_media = sm_list
            if ec_list:
                self.urls_df.loc[url, ECOMMERCE_KEY] = \
                    paste_strings(ec_list, max_length=MAX_CHARFIELD_LENGTH)
            if sm_list:
                self.urls_df.loc[url, SOCIAL_MEDIA_KEY] = \
                    paste_strings(sm_list, max_length=MAX_CHARFIELD_LENGTH)

            if url_analyse.exists:
                self.urls_df.loc[url, BESTAAT_KEY] = True
                self.urls_df.loc[url, SSL_KEY] = url_analyse.req.ssl
                self.urls_df.loc[url, SSL_VALID_KEY] = url_analyse.req.ssl_valid

        return url_analyse

    def collect_web_sites(self):
        """
        Collect all the web sites of a company, scrape the contents to find info, get the
        best matching web site to the company
        """
        min_distance = None
        max_sequence_match = None
        index_string_match = index_distance = None
        self.collection = collections.OrderedDict()
        for i_web, web_row in self.company_urls_df.iterrows():
            # get the url first from the websites table which list all the urls belonging to
            # one kvk search
            url = web_row[URL_KEY]

            # skip all none uls and also the filtered urls
            if url is None or url == "":
                logger.debug("Skipping url because it is None or empty")
                continue
            if self.filter_urls and url not in self.filter_urls:
                logger.debug(f"filter urls is given so skip {url}")
                continue

            # store a list of UrlInfo object with a minimum info the url which was tested
            url_info = UrlInfo(index=i_web, url=url)
            self.collection[url] = url_info

            print_banner(f"Processing {url}")

            # quick check if we can processes this url based on the country code
            suffix = url_info.url_extract.suffix
            if suffix in self.exclude_extensions.index:
                url_info.outside_nl = True
                logger.info(f"Web site {url} has suffix '.{suffix}' Continue ")

            # get the processing time of the last time you did this url from the table
            try:
                processing_time = self.urls_df.loc[url, DATETIME_KEY]
            except KeyError:
                processing_time = None

            if self.force_process or self.rescan_missing_urls:
                url_info.needs_update = True
            else:
                url_info.needs_update = check_if_url_needs_update(processing_time=processing_time,
                                                                  current_time=self.current_time,
                                                                  older_time=self.older_time)
            if url_info.needs_update:
                # if the url needs update, store the current time
                url_info.processing_time = self.current_time
            else:
                url_info.processing_time = processing_time

            # connect to the url and analyse the contents of a static page
            if self.internet_scraping:
                url_analyse = self.scrape_url_and_store_in_dataframes(url, url_info)
            else:
                url_analyse = None

            url_info.url_analyse = url_analyse

            if url_analyse and not url_analyse.exists:
                self.logger.debug(f"url '{url}'' does not exist")
                continue

            # based on the company postcodes and kvknummer and web contents, make a ranking how
            # good the web sides matches the company
            match = UrlCompanyRanking(url, self.company_name_small,
                                      url_extract=url_info.url_extract,
                                      url_analyse=url_analyse,
                                      company_kvk_nummer=self.kvk_nr,
                                      company_postcodes=self.postcodes,
                                      threshold_string_match=self.threshold_string_match,
                                      threshold_distance=self.threshold_distance,
                                      logger=self.logger)

            url_info.match = match

            # update the min max
            if min_distance is None or match.distance < min_distance:
                index_distance = i_web
                min_distance = match.distance

            if max_sequence_match is None or match.string_match > max_sequence_match:
                index_string_match = i_web
                max_sequence_match = match.string_match

            self.logger.debug("   * {} - {}  - {}".format(url, match.ext.domain,
                                                          match.distance))

        if min_distance is None:
            self.company_urls_df = None
        elif index_string_match != index_distance:
            self.logger.warning(
                "Found minimal distance for {}: {}\nwhich differs from "
                "best string match {}: {}".format(index_distance,
                                                  self.collection[url].url,
                                                  index_string_match,
                                                  self.collection[url].url))

    def get_best_matching_web_site(self):
        """
        From all the web sites stored in the data frame web_df, get the best match
        """

        # first fill the web_df columns we need for ranking
        for i_web, (url_key, url_info) in enumerate(self.collection.items()):
            index = url_info.index
            if url_info.url_analyse is None:
                logger.warning("url {url_key} yielded None analyse. Skip to next")
                continue
            exists = url_info.url_analyse.exists
            self.company_urls_df.loc[index, URL_KEY] = url_info.url
            self.company_urls_df.loc[index, EXISTS_KEY] = exists
            if exists:
                self.company_urls_df.loc[index, DISTANCE_KEY] = url_info.match.distance
                self.company_urls_df.loc[index, STRING_MATCH_KEY] = url_info.match.string_match
                self.company_urls_df.loc[index, HAS_POSTCODE_KEY] = url_info.match.has_postcode
                self.company_urls_df.loc[index, HAS_KVK_NR] = url_info.match.has_kvk_nummer
                self.company_urls_df.loc[index, RANKING_KEY] = url_info.match.ranking
                self.company_urls_df.loc[index, DISTANCE_STRING_MATCH_KEY] = \
                    url_info.match.url_match

        # only select the web site which exist
        mask = self.company_urls_df[EXISTS_KEY]

        # create mask for web name distance
        if self.threshold_distance is not None:
            # select all the web sites with a minimum distance or one higher
            m1 = (self.company_urls_df[DISTANCE_KEY] - self.company_urls_df[
                DISTANCE_KEY].min()) <= self.threshold_distance
        else:
            m1 = mask

        # create mask for web string match
        if self.threshold_string_match is not None:
            m2 = self.company_urls_df[STRING_MATCH_KEY] >= self.threshold_string_match
        else:
            m2 = mask

        m3 = self.company_urls_df[HAS_POSTCODE_KEY]
        m4 = self.company_urls_df[HAS_KVK_NR]

        # we mask al the existing web page and keep all pages which are either with
        # a certain string distance (m1) or in case it has either the post code or kvk
        # number we also keep it
        mask = mask & (m1 | m2 | m3 | m4)

        # make a copy of the valid web sides
        self.company_urls_df = self.company_urls_df[mask].copy()

        self.company_urls_df.sort_values([RANKING_KEY, DISTANCE_STRING_MATCH_KEY], inplace=True,
                                         ascending=[False, True])
        self.logger.debug("Sorted list {}".format(self.company_urls_df[[URL_KEY, RANKING_KEY]]))
