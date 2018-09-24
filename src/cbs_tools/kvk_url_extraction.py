import logging
import os
import sys

import peewee as pw
import pandas as pd
import progressbar as pb
import argparse

from cbs_utils.misc import (create_logger, merge_loggers, Timer)


try:
    from cbs_tools import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

CACHE_TYPES = ["msg_pack", "hdf", "sql", "csv", "pkl"]
COMPRESSION_TYPES = [None, "zlib", "blosc"]

KVK_KEY = "kvk_nummer"
NAME_KEY = "naam"
URL_KEY = "url"
COMPANY_KEY = "company"

# set up global logger
logger = create_logger(name=__name__, console_log_format_clean=True)
merge_loggers(logger, "cbs_utils")

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]
PB_MESSAGE_FORMAT = " Processing {} of {}"

# postpone the parsing of the database after we have created the parser class
database = pw.SqliteDatabase(None)


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(pw.Model):
    class Meta:
        database = database


# this class describes the format of the sql data base
class Company(BaseModel):
    naam = pw.CharField(null=True)
    kvk_nummer = pw.CharField(primary_key=True)

    plaats = pw.CharField(null=True)
    postcode = pw.CharField(null=True)
    straat = pw.CharField(null=True)


class WebSite(BaseModel):
    company = pw.ForeignKeyField(Company)
    url = pw.CharField(null=False)
    kvk_nummer = pw.CharField(null=True)
    naam = pw.CharField(null=True)
    validated = pw.BooleanField(default=False)


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

    def __init__(self, url_input_file_name, reset_database=False,
                 compression=None,
                 maximum_entries=None,
                 database_name="kvk_db.sqlite",
                 progressbar=False,
                 kvk_key="KvK",
                 name_key="Name",
                 url_key="URL",
                 n_count_threshold=10,
                 ):

        logger.info("Connecting to database {}".format(database_name))
        database.init(database_name)
        database.connect()

        # make table connections
        self.kvk_key = kvk_key
        self.name_key = name_key
        self.url_key = url_key
        self.kvk_register = Company()

        self.n_count_threshold = n_count_threshold

        self.url_input_file_name = url_input_file_name
        self.reset_database = reset_database

        self.maximum_entries = maximum_entries

        self.compression = compression
        self.progressbar = progressbar

        self.data: pd.DataFrame = None

        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.read_database()

        logger.debug("Read Data Frame info\n{}".format(self.data.info))
        logger.debug("Read Data Frame head\n{}".format(self.data.head()))

        # self.make_report()

    def make_report(self):
        """
        Report all the tables and data we have loaded so far
        """
        number_of_kvk_companies = self.kvk_register.select().count()
        logger.info("Head of all {} kvk entries".format(number_of_kvk_companies))
        for cnt, record in enumerate(self.kvk_register.select().paginate(0)):
            logger.info("{:03d}: {} - {}".format(cnt, record.kvknr, record.handelsnaam))

    def read_database(self):
        """
        Read the URL data base
        """
        file_base, file_ext = os.path.splitext(self.url_input_file_name)
        file_base2, file_ext2 = os.path.splitext(file_base)

        if self.reset_database or not self.kvk_register.table_exists():
            # we are running the script for the first time or we want to reset the cache, so
            # read the original csv data and store it to cache
            logger.info("Reading data from original data base {name}"
                        "".format(name=self.url_input_file_name))
            with Timer(name="read url") as _:
                if ".csv" in (file_ext, file_ext2):
                    self.data = pd.read_csv(self.url_input_file_name,
                                            header=None,
                                            usecols=[1, 2, 4],
                                            names=[self.kvk_key, self.name_key, self.url_key],
                                            nrows=self.maximum_entries)
                else:
                    # add the type so we can recognise it is a data frame
                    self.data: pd.DataFrame = pd.read_hdf(self.url_input_file_name,
                                                          stop=self.maximum_entries)
                    self.data.reset_index(inplace=True)

            # rename the columns to match our tables
            self.data.rename(columns={
                self.kvk_key: KVK_KEY,
                self.url_key: URL_KEY,
                self.name_key: NAME_KEY},
                inplace=True)

            with Timer(name="dump_kvk_url_to_mysql") as _:
                self.dump_kvk_url_to_myqsl()
        else:
            # we have read the csv file before so now we can read from the cache
            with Timer(name="read cache") as _:
                self.read_database_from_cache()

        logger.info("Done reading file")
        logger.debug("Data info")
        logger.debug("Data head")

    def dump_kvk_url_to_myqsl(self):
        """data
        Dump the original list to mysql
        """
        logger.info("Start writing to mysql data base")

        database.drop_tables([Company])
        database.drop_tables([WebSite])
        database.create_tables([Company, WebSite])

        kvk = self.data[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        record_list = list(kvk.to_dict(orient="index").values())
        logger.info("Start writing table urls")
        with Timer(units="s") as _:
            Company.insert_many(record_list).execute()

        # first remove all the urls that occur more the 'n_count_threshold' times.
        urls = self.data
        # this line add the number of occurrences to each url
        #
        n_count = urls.groupby(URL_KEY)[URL_KEY].transform("count")
        url_before = set(urls[URL_KEY].values)
        urls = urls[n_count < self.n_count_threshold]
        url_after = set(urls[URL_KEY].values)
        url_removed = url_before.difference(url_after)
        logger.info("Removed URLS:\n{}".format(url_removed))

        # turn the kvknumber/url combination into the index and remove the duplicates. This
        # means that per company each url only occurs one time
        urls = urls.set_index([KVK_KEY, URL_KEY]).sort_index()
        # this removes all the duplicated indices, i.e. combination kvk_number/url. So if one
        # kvk company has multiple times www.facebook.com at the web site, only is kept.
        urls = urls[~urls.index.duplicated()]

        urls.reset_index(inplace=True)
        urls = urls[[KVK_KEY, URL_KEY, NAME_KEY]]
        url_list = list(urls.to_dict(orient="index").values())
        # add to all urls the object referencing to the Company table
        for web_info in url_list:
            # loop over al the urls and get the company for each url
            kvk_nr = web_info[KVK_KEY]
            # get the link to the company table for this kvk number
            company = Company.get(Company.kvk_nummer == kvk_nr)
            # add the company refernce to the dictionary
            web_info[COMPANY_KEY] = company

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")
        with Timer(units="s") as _:
            WebSite.insert_many(url_list).execute()

        logger.debug("Done")

    def store_database_to_cache(self):
        """
        Store the data base to the cache file. Depending on the *cache_type* atttribute this can be
        either message pack or hdf 5 format
        """

        logger.info("Dumping data to cache file {} ".format(self.cache_url_file_name))
        if self.cache_type == "msg_pack":
            # dump to message pack
            self.data.to_msgpack(self.cache_url_file_name)
        elif self.cache_type == "hdf":
            self.data.to_hdf(self.cache_url_file_name, key="kvk_data", mode="w", dropna=True,
                             format="fixed")
        elif self.cache_type == "sql":
            self.data.to_sql(self.cache_url_file_name)
        elif self.cache_type == "csv":
            self.data.to_csv(self.cache_url_file_name)
        elif self.cache_type == "pkl":
            self.data.to_pickle(self.cache_url_file_name, compression=self.compression)
        else:
            raise AssertionError("Invalid cache type found: {} ".format(self.cache_type))

    def read_database_from_cache(self):
        """
        Read the data base from the cache file
        """

        logger.info("Reading data from cached file {name}".format(name=self.cache_url_file_name))
        if self.cache_type == "msg_pack":
            self.data = pd.read_msgpack(self.cache_url_file_name)
        elif self.cache_type == "hdf":
            self.data = pd.read_hdf(self.cache_url_file_name)
        elif self.cache_type == "sql":
            self.data = pd.read_sql(self.cache_url_file_name)
        elif self.cache_type == "csv":
            self.data = pd.read_sql(self.cache_url_file_name)
        elif self.cache_type == "pkl":
            self.data = pd.read_pickle(self.cache_url_file_name)
        else:
            raise AssertionError("Invalid cache type found: {} ".format(self.cache_type))

    def __exit__(self, *args):
        """
        Make sure to close the database after we are done
        """
        database.close()


def _parse_the_command_line_arguments(args):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # parse the command line to set some options2
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser(description='Parse a CSV file with KVK URLs',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # set the verbosity level command line arguments
    # mandatory arguments
    parser.add_argument("url_input_file_name", action="store",
                        help="The CSV file containing all the URL data")
    parser.add_argument("--version", help="Show the current version", action="version",
                        version="{}\nPart of cbs_tools version {}".format(
                            os.path.basename(__file__), __version__))
    parser.add_argument('-d', '--debug', help="Print lots of debugging statements",
                        action="store_const", dest="log_level", const=logging.DEBUG,
                        default=logging.INFO)
    parser.add_argument('-v', '--verbose', help="Be verbose", action="store_const",
                        dest="log_level", const=logging.INFO)
    parser.add_argument('-q', '--quiet', help="Be quiet: no output", action="store_const",
                        dest="log_level", const=logging.WARNING)
    parser.add_argument('--progressbar', help="Show a progress bar", action="store_true")
    parser.add_argument('--reset_database', help="Reset the data base in case we have generated"
                                                 "a cache file already", action="store_true")
    parser.add_argument('--cache_type', help="Type of the cache file ",
                        choices=CACHE_TYPES, default="hdf")
    parser.add_argument('--compression', help="Type of the compression ",
                        choices=COMPRESSION_TYPES, default=None)
    parser.add_argument('--maximum_entries', help="Maximum number of entries to store", type=int,
                        default=None)

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    logger.setLevel(args.log_level)
    with Timer(message="blabl") as t:
        a = 10
    print(t.delta_time)
    script_name = os.path.basename(sys.argv[0])
    start_time = pd.to_datetime("now")
    logger.info("Start {script} (v: {version}) at {start_time}:{cmd}".format(script=script_name,
                                                                             version=__version__,
                                                                             start_time=start_time,
                                                                             cmd=sys.argv[:]))
    # change the log level to our requested level
    if args.progressbar:
        logger.setLevel(logging.INFO)

    KvKUrlParser(
        url_input_file_name=args.url_input_file_name,
        reset_database=args.reset_database,
        progressbar=args.progressbar,
        compression=args.compression,
        maximum_entries=args.maximum_entries,
    )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
