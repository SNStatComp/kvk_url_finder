import datetime
import difflib
import logging
import multiprocessing as mp
import os
import re
import sys
import time

import Levenshtein
import pandas as pd
import progressbar as pb
import tldextract
from tqdm import tqdm

from cbs_utils.misc import (get_logger, create_logger)
from kvk_url_finder.models import *
from kvk_url_finder import LOGGER_BASE_NAME

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

MAX_SQL_CHUNK = 500

STOP_FILE = "stop"

logger = get_logger(__name__)

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

    # all spacies verwijderen
    naam_small = re.sub("\s+", "", naam_small)

    return naam_small


class KvKRange(object):
    """
    A class holding the range of kvk numbers

    Parameters
    ----------
    kvk_range: dict
        dictionary with two fields:
            * start: int
                Start kvk number to process
            * stop: int
                End kvk number to process
    """

    def __init__(self, kvk_range):
        if kvk_range is not None:
            self.start = kvk_range["start"]
            self.stop = kvk_range["stop"]
        else:
            self.start = None
            self.stop = None


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
                 cache_directory=".",
                 address_input_file_name=None,
                 url_input_file_name=None,
                 kvk_selection_input_file_name=None,
                 kvk_selection_kvk_key=None,
                 kvk_selection_kvk_sub_key=None,
                 address_keys=None,
                 kvk_url_keys=None,
                 reset_database=False,
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
                 ):

        # launch the process
        if i_proc is not None and number_of_processes > 1:
            mp.Process.__init__(self)
            formatter = logging.Formatter("{:2d} ".format(i_proc) +
                                          "%(levelname)-5s : "
                                          "%(message)s "
                                          "(%(filename)s:%(lineno)s)",
                                          datefmt="%Y-%m-%d %H:%M:%S")
            log_file = "{}_{:02d}".format(log_file_base, i_proc)
            logger_name = f"{LOGGER_BASE_NAME}_{i_proc}"
        else:
            formatter = logging.Formatter("%(levelname)-5s : "
                                          "%(message)s "
                                          "(%(filename)s:%(lineno)s)",
                                          datefmt="%Y-%m-%d %H:%M:%S")
            log_file = log_file_base
            logger_name = LOGGER_BASE_NAME

        self.i_proc = i_proc

        self.address_keys = address_keys
        self.kvk_url_keys = kvk_url_keys

        self.save = save

        if i_proc is not None:
            self.logger = create_logger(name=logger_name,
                                        file_log_level=log_level_file,
                                        log_file=log_file,
                                        formatter=formatter)
            self.logger.info("Set up class logger for proc {}".format(i_proc))
        else:
            self.logger = logging.getLogger(logger_name)
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

        self.kvk_selection_input_file_name = kvk_selection_input_file_name
        self.kvk_selection_kvk_key = kvk_selection_kvk_key
        self.kvk_selection_kvk_sub_key = kvk_selection_kvk_sub_key
        self.kvk_selection = None

        self.cache_directory = Path(cache_directory)

        self.impose_url_for_kvk = impose_url_for_kvk

        self.force_process = force_process

        self.n_count_threshold = n_url_count_threshold

        self.url_input_file_name = url_input_file_name
        self.address_input_file_name = address_input_file_name

        self.reset_database = reset_database
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

        self.kvk_range_read = KvKRange(kvk_range_read)

        self.kvk_range_process = KvKRange(kvk_range_process)

        self.url_df: pd.DataFrame = None
        self.addresses_df: pd.DataFrame = None
        self.kvk_df: pd.DataFrame = None

        self.number_of_processes = number_of_processes

        self.kvk_ranges = None

        self.database = init_database(database_name, database_type=database_type)
        tables = init_models(self.database, self.reset_database)
        self.Company = tables[0]
        self.Address = tables[1]
        self.WebSite = tables[2]

    def run(self):
        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.logger.info("Matching the best url's")
        self.find_best_matching_url()

    def generate_sql_tables(self):
        if self.kvk_selection_input_file_name:
            self.read_database_selection()
        self.read_database_addresses()
        self.read_database_urls()
        self.merge_data_base_kvks()

        self.company_kvks_to_sql()
        self.urls_per_kvk_to_sql()
        self.addresses_per_kvk_to_sql()

    def get_kvk_list_per_process(self):
        """
        Get a list of kvk numbers in the query
        """
        query = (self.Company.select(self.Company.kvk_nummer, self.Company.process_nr)
                 .order_by(self.Company.kvk_nummer))
        kvk_to_process = list()
        start = self.kvk_range_process.start
        stop = self.kvk_range_process.stop
        number_in_range = 0
        for q in query:
            if self.maximum_entries is not None and number_in_range >= self.maximum_entries:
                # maximum entries reached
                break
            kvk = q.kvk_nummer
            if start is not None and kvk < start or stop is not None and kvk > stop:
                # skip because is outside range
                continue
            number_in_range += 1
            if not self.force_process and q.process_nr >= 0:
                # skip because we have already processed this record and the 'force' option is False
                continue
            # we can processes this record, so add it to the list
            kvk_to_process.append(kvk)

        n_kvk = len(kvk_to_process)

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
        self.kvk_ranges = list()

        for i_proc in range(self.number_of_processes):
            if i_proc == self.number_of_processes - 1:
                kvk_list = kvk_to_process[i_proc * n_per_proc:]
            else:
                kvk_list = kvk_to_process[i_proc * n_per_proc:(i_proc + 1) * n_per_proc]

            kvk_first = kvk_list[0]
            kvk_last = kvk_list[-1]

            self.kvk_ranges.append(dict(start=kvk_first, stop=kvk_last))

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

        query = self.Company.select()
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
        df2 = self.addresses_df[[KVK_KEY, NAME_KEY]]
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

        n_before = self.addresses_df.index.size
        self.addresses_df.set_index(KVK_KEY, inplace=True)

        # append the new address to the address data base
        self.addresses_df = pd.concat([self.addresses_df, new_kvk_name], axis=0, sort=True)
        self.addresses_df.sort_index(inplace=True)
        self.addresses_df.reset_index(inplace=True)

        n_after = self.addresses_df.index.size
        self.logger.info("Added {} kvk from url list to addresses".format(n_after - n_before))

    # @profile
    def find_best_matching_url(self):
        """
        Per company, see which url matches the best the company name
        """

        start = self.kvk_range_process.start
        stop = self.kvk_range_process.stop
        self.logger.info("Start finding best matching urls for proc {}".format(self.i_proc))

        if start is not None or stop is not None:
            if start is None:
                self.logger.info("Make query from start until stop {}".format(stop))
                query = (self.Company
                         .select().where(self.Company.kvk_nummer <= stop)
                         .prefetch(self.WebSite, self.Address))
            elif stop is None:
                self.logger.info("Make query from start {} until end".format(start))
                query = (self.Company
                         .select().where(self.Company.kvk_nummer >= start)
                         .prefetch(self.WebSite, self.Address))
            else:
                self.logger.info("Make query from start {} until stop {}".format(start, stop))
                query = (self.Company
                         .select()
                         .where(self.Company.kvk_nummer.between(start, stop))
                         .prefetch(self.WebSite, self.Address))
                self.logger.info("Done!")
        else:
            self.logger.info("Make query without selecting in the kvk range")
            query = (self.Company.select()
                     .prefetch(self.WebSite, self.Address))

        # count the number of none-processed queries (ie in which the processed flag == False
        # we have already imposed the max_entries option in the selection of the ranges
        self.logger.info("Counting all")
        maximum_queries = [q.process_nr >= 0 and not self.force_process for q in query].count(False)
        self.logger.info("Maximum queries obtained from selection as {}".format(maximum_queries))

        self.logger.info("Start processing {} queries between {} - {} ".format(maximum_queries,
                                                                               start, stop))

        if self.progressbar and self.showbar:
            pbar = tqdm(total=maximum_queries, position=self.i_proc, file=sys.stdout)
            pbar.set_description("@{:2d}: ".format(self.i_proc))
        else:
            pbar = None

        start = time.time()
        for cnt, company in enumerate(query):

            # first check if we do not have to stop
            if self.maximum_entries is not None and cnt == self.maximum_entries:
                self.logger.info("Maximum entries reached")
                break
            if os.path.exists(STOP_FILE):
                self.logger.info("Stop file found. Quit processing")
                os.remove(STOP_FILE)
                break

            if company.process_nr >= 0 and not self.force_process:
                self.logger.debug("Company {} ({}) already processed. Skipping"
                                  "".format(company.kvk_nummer, company.naam))
                continue

            self.logger.info("Processing {} ({})".format(company.kvk_nummer, company.naam))

            try:
                company_url_match = \
                    CompanyUrlMatch(company,
                                    imposed_urls=self.impose_url_for_kvk,
                                    distance_threshold=self.threshold_distance,
                                    string_match_threshold=self.threshold_string_match,
                                    i_proc=self.i_proc
                                    )
                logger.info("Done with {}".format(company_url_match.company_name))
            except pw.DatabaseError as err:
                self.logger.warning(f"{err}")
                self.logger.warning("skipping")

            if pbar:
                pbar.update()

        if pbar is not None:
            pbar.close()

        duration = time.time() - start
        logger.info(f"Done processing in {duration} seconds")
        # this is not faster than save per record
        # with Timer("Updating tables") as _:
        #    query = (Company.update(dict(url=Company.url, processed=Company.processed)))
        #    query.execute()

    def scrape_url(self, url):
        """
        Scrape the contents of the url

        Parameters
        ----------
        url: str
            url of the website to scrape
        """
        logger.debug("Start scraping")

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
        cache_file = self.cache_directory / (file_base2 + ".pkl")

        if os.path.exists(cache_file):
            # add the type so we can recognise it is a data frame
            logger.info("Reading from cache {}".format(cache_file))
            df: pd.DataFrame = pd.read_pickle(cache_file)
            df.reset_index(inplace=True)
        elif ".csv" in (file_ext, file_ext2):
            logger.info("Reading from file {}".format(file_name))
            df = pd.read_csv(file_name,
                             header=None,
                             usecols=usecols,
                             names=names
                             )

            if remove_spurious_urls:
                logger.info("Removing spurious urls")
                df = self.remove_spurious_urls(df)

            df = self.clip_kvk_range(df, unique_key=unique_key, kvk_range=self.kvk_range_read)

            logger.info("Writing data to cache {}".format(cache_file))
            df.to_pickle(cache_file)
        else:
            raise AssertionError("Can only read h5 or csv files")

        try:
            df.drop("index", axis=0, inplace=True)
        except KeyError:
            logger.debug("No index to drop")
        else:
            logger.debug("Dropped index")

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

        logger.info("Removing duplicated table entries")
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
            logger.info("Selecting kvk number from {} to {}".format(start, stop))
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
        logger.debug("Kept {} out of {} records".format(n_after, n_before))

        # check if we have  any valid entries in the range
        if n_after == 0:
            logger.info(dataframe.info())
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

        self.addresses_df = self.read_csv_input_file(self.address_input_file_name,
                                                     usecols=[col_kvk, col_name, col_adr,
                                                              col_post, col_city],
                                                     names=[KVK_KEY, NAME_KEY, ADDRESS_KEY,
                                                            POSTAL_CODE_KEY, CITY_KEY],
                                                     unique_key=POSTAL_CODE_KEY)
        self.remove_duplicated_kvk_entries()

        logger.debug("Done")

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
        last_website = self.WebSite.select().order_by(self.WebSite.company_id.desc()).get()
        kvk_last = int(last_website.company.kvk_nummer)

        try:
            # based on the last kvk in the database, get the index in the csv file
            # note that with the loc selection we get all URL's belongin to this kvk. Therfore
            # take the last of this list with -1
            row_index = self.url_df.loc[self.url_df[KVK_KEY] == kvk_last].index[-1]
        except IndexError:
            logger.debug("No last index found.  n_entries to skip to {}".format(n_skip_entries))
        else:
            # we have the last row index. This means that we can add this index to the n_entries
            # we have used now. Return this n_entries
            last_row = self.url_df.loc[row_index]
            logger.debug("found: {}".format(last_row))
            n_skip_entries += row_index + 1
            logger.debug("Updated n_entries to skip to {}".format(n_skip_entries))

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
        logger.debug("Removed URLS:\n{}".format(url_removed))

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

        nr = self.addresses_df.index.size
        logger.info("Removing duplicated kvk entries")
        query = self.Company.select()
        kvk_list = list()
        try:
            for company in query:
                kvk_nummer = int(company.kvk_nummer)
                if kvk_nummer in self.addresses_df[KVK_KEY].values:
                    kvk_list.append(company.kvk_nummer)
        except pw.OperationalError:
            # nothing to remove
            return

        kvk_in_db = pd.DataFrame(data=kvk_list, columns=[KVK_KEY])

        kvk_in_db.set_index(KVK_KEY, inplace=True)
        kvk_to_remove = self.addresses_df.set_index(KVK_KEY)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove.index.duplicated()]
        kvk_to_remove = kvk_to_remove.reindex(kvk_in_db.index)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove[NAME_KEY].isnull()]
        try:
            self.addresses_df = self.addresses_df.set_index(KVK_KEY).drop(index=kvk_to_remove.index)
        except KeyError:
            logger.debug("Nothing to drop")
        else:
            self.addresses_df.reset_index(inplace=True)

    # @profile
    def remove_duplicated_url_entries(self):
        """
        Remove all the companies/url combination which already have been stored in
        the sql tables

        """

        # based on the data in the WebSite table create a data frame with all the kvk which
        # we have already included. These can be removed from the data we have just read
        nr = self.url_df.index.size
        logger.info("Removing duplicated kvk/url combinies. Data read at start: {}".format(nr))
        logger.debug("Getting all sql websides from database")
        kvk_list = list()
        url_list = list()
        name_list = list()
        query = (self.Company
                 .select()
                 .prefetch(self.WebSite)
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
        logger.debug("Dropping all duplicated web sides")
        kvk_to_remove = self.url_df.set_index([KVK_KEY, URL_KEY])
        kvk_to_remove = kvk_to_remove.reindex(kvk_in_db.index)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove[NAME_KEY].isnull()]
        try:
            self.url_df = self.url_df.set_index([KVK_KEY, URL_KEY]).drop(index=kvk_to_remove.index)
        except KeyError:
            logger.debug("Nothing to drop")
        else:
            self.url_df.reset_index(inplace=True)

        logger.debug("Getting all  companies in Company table")
        kvk_list = list()
        name_list = list()
        for company in self.Company.select():
            kvk_list.append(int(company.kvk_nummer))
            name_list.append(company.naam)
        companies_in_db = pd.DataFrame(data=list(zip(kvk_list, name_list)),
                                       columns=[KVK_KEY, NAME_KEY])
        companies_in_db.set_index([KVK_KEY], drop=True, inplace=True)

        logger.debug("Dropping all  duplicated companies")
        comp_df = self.url_df.set_index([KVK_KEY, URL_KEY])
        comp_df.drop(index=companies_in_db.index, level=0, inplace=True)
        self.url_df = comp_df.reset_index()

        nr = self.url_df.index.size
        logger.debug("Removed duplicated kvk/url combies. Data at end: {}".format(nr))

    # @profile
    def company_kvks_to_sql(self):
        """
        Write all the company kvk with name to the sql
        """
        logger.info("Start writing to mysql data base")
        if self.addresses_df.index.size == 0:
            logger.debug("Empty addresses data frame. Nothing to write")
            return

        self.kvk_df = self.addresses_df[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        record_list = list(self.kvk_df.to_dict(orient="index").values())
        logger.info("Start writing table urls")

        n_batch = int(len(record_list) / MAX_SQL_CHUNK) + 1
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_batch)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None

        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                logger.info("Company chunk nr {}/{}".format(cnt + 1, n_batch))
                self.Company.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
                    sys.stdout.flush()

        if progress:
            progress.finish()
        logger.debug("Done with company table")

    # @profile
    def urls_per_kvk_to_sql(self):
        """
        Write all URL per kvk to the WebSite Table in sql
        """
        logger.debug("Start writing the url to sql")

        if self.url_df.index.size == 0:
            logger.debug("Empty url data  from. Nothing to add to the sql database")
            return

        # create selection of data columns
        urls = self.url_df[[KVK_KEY, URL_KEY, NAME_KEY]]
        urls.loc[:, COMPANY_KEY] = None
        urls.loc[:, BEST_MATCH_KEY] = False
        urls.loc[:, LEVENSHTEIN_KEY] = -1
        urls.loc[:, STRING_MATCH_KEY] = -1
        urls.sort_values([KVK_KEY], inplace=True)
        # count the number of urls per kvk
        n_url_per_kvk = urls.groupby(KVK_KEY)[KVK_KEY].count()

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        kvk_list = self.addresses_df[KVK_KEY].tolist()
        company_vs_kvk = self.Company.select()
        n_comp = company_vs_kvk.count()

        kvk_comp_list = list()
        for company in company_vs_kvk:
            kvk_comp_list.append(int(company.kvk_nummer))
        kvk_comp = set(kvk_comp_list)
        kvk_not_in_addresses = set(kvk_list).difference(kvk_comp)

        logger.info(f"Found: {n_comp} companies")
        wdg = PB_WIDGETS
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_comp)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_comp, fd=sys.stdout).start()
        else:
            progress = None

        company_list = list()
        for counter, company in enumerate(company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            # we need to check if this kvk is in de address list  still
            if kvk_nr in kvk_not_in_addresses:
                logger.debug(f"Skipping kvk {kvk_nr} as it is not in the addresses")
                continue

            try:
                n_url = n_url_per_kvk.loc[kvk_nr]
            except KeyError:
                continue

            # add the company number of url time to the list
            company_list.extend([company] * n_url)

            if progress:
                wdg[-1] = progress_bar_message(counter, n_comp, kvk_nr, company.naam)
                progress.update(counter)
            if counter % MAX_SQL_CHUNK == 0:
                logger.info(" Added {} / {}".format(counter, n_comp))
        if progress:
            progress.finish()

        urls[COMPANY_KEY] = company_list

        # in case there is a None at a row, remove it (as there is not company found)
        urls.dropna(axis=0, inplace=True)

        # the kvk key is already visible via the company_id
        urls.drop([KVK_KEY], inplace=True, axis=1)

        logger.info("Converting urls to dict. This make take some time...")
        url_list = list(urls.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")
        n_batch = int(len(url_list) / MAX_SQL_CHUNK) + 1
        if self.progressbar:
            wdg[-1] = progress_bar_message(0, n_batch)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None
        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(url_list, MAX_SQL_CHUNK)):
                logger.info("URL chunk nr {}/{}".format(cnt + 1, n_batch))
                self.WebSite.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
        if progress:
            progress.finish()

        logger.debug("Done")

    # @profile
    def addresses_per_kvk_to_sql(self):
        """
        Write all address per kvk to the Addresses Table in sql
        """

        logger.info("Start writing the addressees to the sql Addresses table")
        if self.addresses_df.index.size == 0:
            logger.debug("Empty address data frame. Nothing to write")
            return

        # create selection of data columns
        self.addresses_df[COMPANY_KEY] = None
        columns = [KVK_KEY, NAME_KEY, ADDRESS_KEY, POSTAL_CODE_KEY, CITY_KEY, COMPANY_KEY]
        df = self.addresses_df[columns].copy()
        df.sort_values([KVK_KEY], inplace=True)
        # count the number of urls per kvk
        n_url_per_kvk = df.groupby(KVK_KEY)[KVK_KEY].count()

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        logger.info("Adding companies to addresses table")
        kvk_list = self.addresses_df[KVK_KEY].tolist()
        company_vs_kvk = self.Company.select()
        kvk_comp_list = list()
        for company in company_vs_kvk:
            kvk_comp_list.append(int(company.kvk_nummer))
        kvk_comp = set(kvk_comp_list)
        kvk_not_in_addresses = set(kvk_list).difference(kvk_comp)

        idx = pd.IndexSlice
        n_comp = company_vs_kvk.count()
        wdg = None
        if self.progressbar:
            wdg = PB_WIDGETS
            wdg[-1] = progress_bar_message(0, n_comp)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_comp, fd=sys.stdout).start()
        else:
            progress = None

        company_list = list()
        for counter, company in enumerate(company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            if kvk_nr in kvk_not_in_addresses:
                logger.debug(f"Skipping kvk {kvk_nr} as it is not in the addresses")
                continue

            try:
                n_url = n_url_per_kvk.loc[kvk_nr]
            except KeyError:
                continue
            company_list.extend([company] * n_url)

            if counter % MAX_SQL_CHUNK == 0:
                logger.info(" Added {} / {}".format(counter, n_comp))

            if progress:
                wdg[-1] = progress_bar_message(counter, n_comp, kvk_nr, company.naam)
                progress.update(counter)

        if progress:
            progress.finish()

        df[COMPANY_KEY] = company_list

        # the kvk key is already visible via the company_id
        df.drop([KVK_KEY], inplace=True, axis=1)

        logger.info("Converting urls to dict. This make take some time...")
        address_list = list(df.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")
        n_batch = int(len(address_list) / MAX_SQL_CHUNK) + 1
        if self.progressbar:
            wdg = PB_WIDGETS
            wdg[-1] = progress_bar_message(0, n_comp)
            progress = pb.ProgressBar(widgets=wdg, maxval=n_batch, fd=sys.stdout).start()
        else:
            progress = None
        with self.database.atomic():
            for cnt, batch in enumerate(pw.chunked(address_list, MAX_SQL_CHUNK)):
                logger.info("URL chunk nr {}/{}".format(cnt + 1, n_batch))
                self.Address.insert_many(batch).execute()
                if progress:
                    wdg[-1] = progress_bar_message(cnt, n_batch)
                    progress.update(cnt)
        if progress:
            progress.finish()


class ScrapeCompany(object):
    """
    Scrape this url
    """

    def __init__(self, url: str):
        self.url = url
        self.logger = logging.getLogger(__name__)

        self.logger.info("Start scrapnig {}".format(self.url))


class CompanyUrlMatch(object):

    def __init__(self, company, imposed_urls: dict = None,
                 distance_threshold: int = 10,
                 string_match_threshold: float = 0.5,
                 save: bool = True,
                 i_proc=0):

        self.logger = get_logger(__name__)
        self.save = save
        self.i_proc = i_proc

        self.kvk_nr = company.kvk_nummer
        self.company_name: str = company.naam

        self.company = company

        # the impose_url_for_kvk dictionary gives all the kvk numbers for which we just want to
        # impose a url
        self.impose_url = imposed_urls.get(self.kvk_nr)

        self.logger.info("Get Url collection....")
        self.urls = UrlCollection(company, self.company_name, self.kvk_nr,
                                  threshold_distance=distance_threshold,
                                  threshold_string_match=string_match_threshold
                                  )
        self.find_match_for_company()

    def find_match_for_company(self):

        postcodes = list()
        for address in self.company.address:
            self.logger.debug("Found postcode {}".format(address.postcode))
            postcodes.append(address.postcode)

        # only select the close matches
        if self.urls.web_df is not None:

            # the first row in the data frame is the best matching web site
            web_df_best = self.urls.web_df.head(1)

            # store the best matching web site
            self.logger.debug("Best matching url: {}".format(self.urls.web_df.head(1)))
            web_match_index = web_df_best.index.values[0]
            web_match = self.urls.company_websites[web_match_index]
            web_match.best_match = True
            web_match.url = web_df_best["url"].values[0]
            web_match.ranking = web_df_best["ranking"].values[0]
            self.logger.debug("Best matching url: {}".format(web_match.url))

            # update all the properties
            if self.save:
                for web in self.company.websites:
                    web.save()
            self.company.url = web_match.url
            self.company.process_nr = self.i_proc
            self.company.process_time = datetime.datetime.now()
            if self.save:
                self.company.save()


class UrlCollection(object):
    """
    Analyses all url
    """

    def __init__(self, company,
                 company_name: str,
                 kvk_nr: int,
                 threshold_distance: int = 10,
                 threshold_string_match: float = 0.5,
                 impose_url: str = None):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Collect urls {}".format(company_name))

        self.kvk_nr = kvk_nr
        self.company = company
        self.company_name = company_name
        self.company_websites = self.company.websites
        self.company_name_small = clean_name(self.company_name)

        self.threshold_distance = threshold_distance
        self.threshold_string_match = threshold_string_match

        number_of_websites = len(self.company_websites)
        self.web_df = pd.DataFrame(index=range(number_of_websites),
                                   columns=["url", "distance", "string_match",
                                            "subdomain", "domain", "suffix", "ranking"])

        # remove space and put to lower for better comparison with the url
        self.logger.info(
            "Checking {}: {} ({})".format(self.kvk_nr, self.company_name, self.company_name_small))
        self.collect_web_sites()

        self.logger.info("Get best match")
        if impose_url:
            # just select the url to impose
            self.web_df = self.web_df[self.web_df["url"] == impose_url].copy()
        elif self.web_df is not None:
            self.get_best_matching_web_site()
            self.logger.info("Best Match".format(self.web_df.head(1)))
        else:
            self.logger.info("No website found for".format(self.company_name))

    def collect_web_sites(self):
        """
        Collect all the web sites of a company and store it in a data frame
        """
        min_distance = None
        max_sequence_match = None
        index_string_match = index_distance = None
        for i_web, web in enumerate(self.company_websites):
            ext = tldextract.extract(web.url)

            # the subdomain may also contain the relevant part, e.g. for ramlehapotheek.leef.nl,
            # the sub domain is ramlehapotheek, which is closer to the company name the the
            # domain leef. Therefore pick the minimum
            subdomain_dist = Levenshtein.distance(ext.subdomain, self.company_name_small)
            domain_dist = Levenshtein.distance(ext.domain, self.company_name_small)
            distance = min(subdomain_dist, domain_dist)
            web.levenshtein = distance

            # also we are going to match the sequences. The match range between 0 (no match) and 1
            # (full match)
            subdomain_match = difflib.SequenceMatcher(None, ext.subdomain,
                                                      self.company_name_small).ratio()
            domain_match = difflib.SequenceMatcher(None, ext.domain,
                                                   self.company_name_small).ratio()
            string_match = max(subdomain_match, domain_match)
            web.string_match = string_match

            web.best_match = False

            if min_distance is None or distance < min_distance:
                index_distance = i_web
                min_distance = distance

            if max_sequence_match is None or string_match > max_sequence_match:
                index_string_match = i_web
                max_sequence_match = string_match

            self.web_df.loc[i_web, :] = [web.url, distance, string_match,
                                         ext.subdomain, ext.domain, ext.suffix, 0]

            logger.debug("   * {} - {}  - {}".format(web.url, ext.domain, distance))

        if min_distance is None:
            self.web_df = None
        elif index_string_match != index_distance:
            logger.warning("Found minimal distance for {}: {}\nwhich differs from best string "
                           "match {}: {}".format(index_distance,
                                                 self.web_df.loc[index_distance, "url"],
                                                 index_string_match,
                                                 self.web_df.loc[index_string_match, "url"]))

    def get_best_matching_web_site(self):
        """
        From all the web sites stored in the data frame web_df, get the best match
        """

        if self.threshold_distance is not None:
            # select all the web sites with a minimum distance or one higher
            mask = (self.web_df["distance"] - self.web_df[
                "distance"].min()) <= self.threshold_distance
            self.web_df = self.web_df[mask].copy()

        if self.threshold_string_match is not None:
            df = self.web_df[self.web_df["string_match"] >= self.threshold_string_match].copy()
            if df.empty:
                # the filter with the string match threshold give an empty data frame. Just take the
                # best match
                df = self.web_df.sort_values(["string_match"], ascending=False)
                self.web_df = df.copy().head(1)
            else:
                self.web_df = df

        def rate_it(column_name, ranking, value="www", score=1):
            """
            In case the column 'column_name' has a value equal to 'value' add the 'score
            to the current 'ranking' and return the result
            """
            return ranking + score if column_name == value else ranking

        # loop over the subdomains and add the score in case we have a web site with this
        # sub domain. Do the same after that for the prefixes
        for subdomain, score in [("www", 1), ("", 1), ("https", 1), ("http", 1)]:
            self.web_df["ranking"] = self.web_df.apply(
                lambda x: rate_it(x.subdomain, x.ranking, value=subdomain, score=score),
                axis=1)
        for suffix, score in [("com", 3), ("nl", 2), ("org", 1), ("eu", 1)]:
            self.web_df["ranking"] = self.web_df.apply(
                lambda x: rate_it(x.suffix, x.ranking, value=suffix, score=score), axis=1)

        # sort first on the ranking, then on the distance
        self.web_df.sort_values(["ranking", "distance", "string_match"],
                                ascending=[False, True, False],
                                inplace=True)
