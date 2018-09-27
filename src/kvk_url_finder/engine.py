import logging
import os
import tldextract
import Levenshtein

import pandas as pd
import progressbar as pb

from cbs_utils.misc import get_logger

from kvk_url_finder.models import *

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

try:
    # if profile exist, it means we are running kernprof to time all the lines of the functions
    # decorated with @profile
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

MAX_SQL_VARIABLES = 99999
MAX_SQL_CHUNK = 1000

logger = get_logger(__name__)

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]
PB_MESSAGE_FORMAT = " Processing {} of {}"


def progress_bar_message(cnt, total):
    return "Processed time {:d} of {:d}".format(cnt + 1, total)


class KvKUrlParser(object):
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
                 address_input_file_name=None,
                 url_input_file_name=None,
                 address_keys=None,
                 kvk_url_keys=None,
                 reset_database=False,
                 extend_database=False,
                 compression=None,
                 maximum_entries=None,
                 database_name="kvk_db.sqlite",
                 progressbar=False,
                 n_url_count_threshold=10,
                 force_process=False,
                 update_sql_tables=False,
                 ):

        self.address_keys = address_keys
        self.kvk_url_keys = kvk_url_keys

        self.force_process = force_process

        self.n_count_threshold = n_url_count_threshold

        self.url_input_file_name = url_input_file_name
        self.address_input_file_name = address_input_file_name

        self.reset_database = reset_database
        self.extend_database = extend_database

        self.maximum_entries = maximum_entries

        self.compression = compression
        self.progressbar = progressbar

        self.url_df: pd.DataFrame = None
        self.addresses_df: pd.DataFrame = None


        logger.info("Connecting to database {}".format(database_name))
        database.init(database_name)
        database.connect()
        database.create_tables([Company, Address, WebSite])
        if self.reset_database:
            database.drop_tables([Company, Address, WebSite])

        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        if update_sql_tables:
            self.read_database_urls()
            self.read_database_addresses()

            self.company_kvks_to_sql()
            self.urls_per_kvk_to_sql()
            self.addresses_per_kvk_to_sql()
        else:
            logger.debug("Skip updating the sql tables")

        self.find_best_matching_url()

    @profile
    def find_best_matching_url(self):
        """
        Per company, see which url matches the best the company name
        """

        query = (Company
                 .select()
                 .prefetch(WebSite)
                 )
        for cnt, company in enumerate(query):

            kvk_nr = company.kvk_nummer
            naam = company.naam
            if company.processed and not self.force_process:
                logger.debug("Company {} ({}) already processed. Skipping".format(kvk_nr, naam))
                continue

            logger.info("Checking {} : {} {}".format(cnt, kvk_nr, naam))

            min_distance = None
            web_match = None
            for web in company.websites:
                ext = tldextract.extract(web.url)

                domain = ext.domain

                distance = Levenshtein.distance(domain, naam)

                web.levenshtein = distance

                if min_distance is None or distance < min_distance:
                    min_distance = distance
                    web_match = web

                logger.debug("   * {} - {}  - {}".format(web.url, domain, distance))

            logger.debug("Best matching url: {}".format(web_match.url))
            web_match.best_match = True

            # update all the properties
            for web in company.websites:
                logger.debug("Updating web site properties")
                web.save()
            company.url = web_match.url
            company.processed = True
            company.save()

            if self.maximum_entries is not None and cnt == self.maximum_entries:
                logger.info("Maximum entries reached")
                break

    def read_csv_input_file(self,
                            file_name: str,
                            usecols: list = None,
                            names: list = None,
                            remove_spurious_urls=False
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


        Returns
        -------
        DataFrame:
            Dataframe with the data

        """

        file_base, file_ext = os.path.splitext(file_name)
        file_base2, file_ext2 = os.path.splitext(file_base)

        cache_file = file_base2 + ".h5"

        if os.path.exists(cache_file):
            # add the type so we can recognise it is a data frame
            logger.info("Reading urls from cache {}".format(cache_file))
            df: pd.DataFrame = pd.read_hdf(cache_file)
            df.reset_index(inplace=True)
        elif ".csv" in (file_ext, file_ext2):
            logger.info("Reading urls from file {}".format(self.url_input_file_name))
            df = pd.read_csv(self.url_input_file_name,
                             header=None,
                             usecols=usecols,
                             names=names
                             )

            if remove_spurious_urls:
                logger.info("Removing spurious urls")
                df = self.remove_spurious_urls(df)

            logger.info("Writing urls to cache {}".format(cache_file))
            df.to_hdf(cache_file, "w")
        else:
            raise AssertionError("Can only read h5 or csv files")

        return df

    @profile
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
                                               remove_spurious_urls=True)

        logger.info("Removing duplicated table entries")
        self.remove_duplicated_entries()

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
                                                            POSTAL_CODE_KEY, CITY_KEY])
        logger.debug("Done")

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
        last_website = WebSite.select().order_by(WebSite.company_id.desc()).get()
        kvk_last = int(last_website.company.kvk_nummer)

        col_kvk = self.kvk_url_keys[KVK_KEY]
        col_name = self.kvk_url_keys[NAME_KEY]
        col_url = self.kvk_url_keys[URL_KEY]

        # based on the size of the total websites in the data base set the start of reading
        # at n_entries. Perhaps we have to read from the csv file furhter in case we have dropped
        # kvk before. This is what we are going to find out now
        logger.debug("Start reading full url input file {}".format(self.url_input_file_name))
        tmp_data = pd.read_csv(self.url_input_file_name,
                               header=None,
                               usecols=[col_kvk, col_name, col_url],
                               names=[KVK_KEY, NAME_KEY, URL_KEY],
                               nrows=self.maximum_entries,
                               skiprows=n_skip_entries)

        try:
            # based on the last kvk in the database, get the index in the csv file
            # note that with the loc selection we get all URL's belongin to this kvk. Therfore
            # take the last of this list with -1
            row_index = tmp_data.loc[tmp_data[KVK_KEY] == kvk_last].index[-1]
        except IndexError:
            logger.debug("No last index found.  n_entries to skip to {}".format(n_skip_entries))
        else:
            # we have the last row index. This means that we can add this index to the n_entries
            # we have used now. Return this n_entries
            last_row = tmp_data.loc[row_index]
            logger.debug("found: {}".format(last_row))
            n_skip_entries += row_index + 1
            logger.debug("Updated n_entries to skip to {}".format(n_skip_entries))

        return n_skip_entries

    @profile
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

    @profile
    def remove_duplicated_entries(self):
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
        query = (Company
                 .select()
                 .prefetch(WebSite)
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
        for company in Company.select():
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

    @profile
    def company_kvks_to_sql(self):
        """
        Write all the company kvk with name to the sql
        """
        logger.info("Start writing to mysql data base")

        kvk = self.url_df[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        record_list = list(kvk.to_dict(orient="index").values())
        logger.info("Start writing table urls")

        n_batch = int(len(record_list) / MAX_SQL_CHUNK) + 1
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                logger.info("Company chunk nr {}/{}".format(cnt + 1, n_batch))
                Company.insert_many(batch).execute()
        logger.debug("Done with company table")

    @profile
    def urls_per_kvk_to_sql(self):
        """
        Write all URL per kvk to the WebSite Table in sql
        """

        # create selection of data columns
        urls = self.url_df[[KVK_KEY, URL_KEY, NAME_KEY]]
        urls[COMPANY_KEY] = None
        urls[BEST_MATCH_KEY] = False
        urls[LEVENSHTEIN_KEY] = -1
        urls.set_index([KVK_KEY, URL_KEY], inplace=True)

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        logger.info("Adding companies to url table")
        company_vs_kvk = Company.select().where(Company.kvk_nummer << self.url_df[KVK_KEY].tolist())
        n_comp = len(company_vs_kvk)
        for counter, company in enumerate(company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            urls.loc[[kvk_nr, ], COMPANY_KEY] = company
            if counter % MAX_SQL_CHUNK == 0:
                logger.info(" Added {} / {}".format(counter, n_comp))

        urls.reset_index(inplace=True)

        # the kvk key is already visible via the company_id
        urls.drop([KVK_KEY], inplace=True, axis=1)

        logger.info("Converting urls to dict. This make take some time...")
        url_list = list(urls.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")
        n_batch = int(len(url_list) / MAX_SQL_CHUNK) + 1
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(url_list, MAX_SQL_CHUNK)):
                logger.info("URL chunk nr {}/{}".format(cnt + 1, n_batch))
                WebSite.insert_many(batch).execute()

        logger.debug("Done")

    @profile
    def addresses_per_kvk_to_sql(self):
        """
        Write all address per kvk to the Addresses Table in sql
        """

        # create selection of data columns
        urls = self.url_df[[KVK_KEY, URL_KEY, NAME_KEY]]
        urls[COMPANY_KEY] = None
        urls[BEST_MATCH_KEY] = False
        urls[LEVENSHTEIN_KEY] = -1
        urls.set_index([KVK_KEY, URL_KEY], inplace=True)

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        logger.info("Adding companies to url table")
        company_vs_kvk = Company.select().where(Company.kvk_nummer << self.url_df[KVK_KEY].tolist())
        n_comp = len(company_vs_kvk)
        for counter, company in enumerate(company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            urls.loc[[kvk_nr, ], COMPANY_KEY] = company
            if counter % MAX_SQL_CHUNK == 0:
                logger.info(" Added {} / {}".format(counter, n_comp))

        urls.reset_index(inplace=True)

        # the kvk key is already visible via the company_id
        urls.drop([KVK_KEY], inplace=True, axis=1)

        logger.info("Converting urls to dict. This make take some time...")
        url_list = list(urls.to_dict(orient="index").values())

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")
        n_batch = int(len(url_list) / MAX_SQL_CHUNK) + 1
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(url_list, MAX_SQL_CHUNK)):
                logger.info("URL chunk nr {}/{}".format(cnt + 1, n_batch))
                WebSite.insert_many(batch).execute()

    def __exit__(self, *args):
        """
        Make sure to close the database after we are done
        """
        database.close()
