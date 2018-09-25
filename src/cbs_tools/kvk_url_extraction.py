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

MAX_SQL_VARIABLES = 99999
MAX_SQL_CHUNK = 10

KVK_KEY = "kvk_nummer"
NAME_KEY = "naam"
URL_KEY = "url"
COMPANY_KEY = "company"

# set up global logger
logger: logging.Logger = None

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

    def __init__(self, url_input_file_name,
                 reset_database=False,
                 extend_database=True,
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
        self.extend_database = extend_database

        self.maximum_entries = maximum_entries

        self.compression = compression
        self.progressbar = progressbar

        self.data: pd.DataFrame = None

        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        if self.reset_database or self.extend_database:
            self.read_database()
        else:
            logger.debug("No need to read. We are already connected")

        self.process_the_urls()

    def process_the_urls(self):
        """
        Per company, check all the urls
        """

        for company in Company.select():
            logger.info("Checking {}".format(company.naam))

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
        Read the URL data from the csv file or hd5 file
        """
        file_base, file_ext = os.path.splitext(self.url_input_file_name)
        file_base2, file_ext2 = os.path.splitext(file_base)

        database.create_tables([Company, WebSite])
        if self.reset_database:
            database.drop_tables([Company])
            database.drop_tables([WebSite])

        if self.extend_database:
            n_entries = len(WebSite.select())
        else:
            n_entries = 0

        # we are running the script for the first time or we want to reset the cache, so
        # read the original csv data and store it to cache
        logger.info("Reading data from original data base {name}"
                    "".format(name=self.url_input_file_name))
        with Timer(name="read url", units="s") as _:
            if ".csv" in (file_ext, file_ext2):
                self.data = pd.read_csv(self.url_input_file_name,
                                        header=None,
                                        usecols=[1, 2, 4],
                                        names=[self.kvk_key, self.name_key, self.url_key],
                                        nrows=self.maximum_entries,
                                        skiprows=n_entries)
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

        logger.info("Removing duplicated table entries")
        self.remove_duplicated_entries()
        logger.debug("Done")

        with Timer(name="dump_kvk_url_to_mysql", units="s") as _:
            self.dump_kvk_url_to_myqsl()

    def remove_duplicated_entries(self):
        """
        Remove all the companies/url combination which already have been stored in
        the sql tables

        """

        # based on the data in the WebSite table create a data frame with all the kvk which
        # we have already included. These can be removed from the data we have just read
        n_entries = len(WebSite.select())
        kvk_in_db = pd.DataFrame(index=range(n_entries), columns=[KVK_KEY, URL_KEY, NAME_KEY])
        for ii, ws in enumerate(WebSite.select()):
            kvk_in_db.loc[ii, KVK_KEY] = int(ws.kvk_nummer)
            kvk_in_db.loc[ii, URL_KEY] = ws.url
            kvk_in_db.loc[ii, NAME_KEY] = ws.naam

        kvk_in_db.set_index([KVK_KEY, URL_KEY], drop=True, inplace=True)

        # drop all the kvk number which we already have loaded
        kvk_df = self.data.set_index([KVK_KEY, URL_KEY])
        kvk_df = kvk_df.reindex(kvk_in_db.index)
        kvk_df = kvk_df[~kvk_df[NAME_KEY].isnull()]
        try:
            self.data = self.data.set_index([KVK_KEY, URL_KEY]).drop(index=kvk_df.index)
        except KeyError:
            logger.debug("Nothing to drop")
        else:
            self.data.reset_index(inplace=True)


        n_companies = len(Company.select())
        companies_in_db = pd.DataFrame(index=range(n_companies), columns=[KVK_KEY])
        for ii, company in enumerate(Company.select()):
            companies_in_db.loc[ii, KVK_KEY] = int(company.kvk_nummer)
            companies_in_db.loc[ii, NAME_KEY] = company.naam
        companies_in_db.set_index([KVK_KEY], drop=True, inplace=True)

        comp_df = self.data.set_index([KVK_KEY, URL_KEY])
        comp_df.drop(index=companies_in_db.index, level=0, inplace=True)
        self.data = comp_df.reset_index()

    def dump_kvk_url_to_myqsl(self):
        """data
        Dump the original list to mysql
        """
        logger.info("Start writing to mysql data base")

        kvk = self.data[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        record_list = list(kvk.to_dict(orient="index").values())
        logger.info("Start writing table urls")
        remove = list()
        for company in Company.select():
            kvk = int(company.kvk_nummer)
            if kvk in self.data[KVK_KEY].values:
                logger.debug("Removing double {}".format(kvk))
                remove.append(kvk)

        logger.info(remove)
        n_batch = len(record_list) / MAX_SQL_CHUNK
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                msg = "Company chunk nr {}/{}".format(cnt, n_batch)
                with Timer(message=msg) as _:
                    Company.insert_many(batch).execute()

        # first remove all the urls that occur more the 'n_count_threshold' times.
        urls = self.data
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
        urls = urls[[KVK_KEY, URL_KEY, NAME_KEY]]
        logger.debug("Converting urls to dict")
        url_list = list(urls.to_dict(orient="index").values())
        # add to all urls the object referencing to the Company table
        for web_info in url_list:
            # loop over al the urls and get the company for each url
            kvk_nr = web_info[KVK_KEY]
            # get the link to the company table for this kvk number
            company = Company.get(Company.kvk_nummer == kvk_nr)
            # add the company reference to the dictionary
            web_info[COMPANY_KEY] = company

        # turn the list of dictionaries into a sql table
        logger.info("Start writing table urls")

        n_batch = len(url_list) / MAX_SQL_CHUNK
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(url_list, MAX_SQL_CHUNK)):
                msg = "Company chunk nr {}/{}".format(cnt, n_batch)
                with Timer(message=msg) as _:
                    WebSite.insert_many(batch).execute()

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
    parser.add_argument("--write_log_to_file", action="store_true",
                        help="Write the logging information to file")
    parser.add_argument("--log_file_base", default="log", help="Default name of the logging output")
    parser.add_argument('--log_file_verbose', help="Be verbose to file", action="store_const",
                        dest="log_level_file", const=logging.INFO)
    parser.add_argument('--log_file_quiet', help="Be quiet: no output to file",
                        action="store_const", dest="log_level_file", const=logging.WARNING)
    parser.add_argument("--progress_bar", action="store_true", default=False,
                        help="Just show a progress bar instaad of the logging message.)")
    parser.add_argument("--no_progress_bar", action="store_false", dest="progress_bar",
                        help="Do not show the progress bar but generate logging information.)")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


def setup_logging(write_log_to_file=False,
                  log_file_base="log",
                  log_level_file=logging.INFO,
                  log_level=None,
                  progress_bar=False,
                  ):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Initialise the logging system
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    if write_log_to_file:
        # http://stackoverflow.com/questions/29087297/
        # is-there-a-way-to-change-the-filemode-for-a-logger-object-that-is-not-configured
        sys.stderr = open(log_file_base + ".err", 'w')
    else:
        log_file_base = None

    _logger = create_logger(file_log_level=log_level_file,
                            console_log_level=log_level,
                            log_file=log_file_base)

    if progress_bar:
        # switch of all logging because we are showing the progress bar via the print statement
        # logger.disabled = True
        # logger.disabled = True
        # logger.setLevel(logging.CRITICAL)
        for handle in _logger.handlers:
            try:
                getattr(handle, "baseFilename")
            except AttributeError:
                # this is the stream handle because we get an AtrributeError. Set it to critical
                handle.setLevel(logging.CRITICAL)

    # with this call we merge the settings of our logger with the logger in the cbs_utils logger
    # so we can control the output
    merge_loggers(_logger, "cbs_utils")

    _logger.info("{:10s}: {}".format("Running", sys.argv))
    _logger.info("{:10s}: {}".format("Version", __version__))
    _logger.info("{:10s}: {}".format("Directory", os.getcwd()))
    _logger.debug("Debug message")

    return _logger


def max_sql_variables():
    """Get the maximum number of arguments allowed in a query by the current
    sqlite3 implementation. Based on `this question
    `_

    Returns
    -------
    int
        inferred SQLITE_MAX_VARIABLE_NUMBER
    """
    import sqlite3
    db = sqlite3.connect(':memory:')
    cur = db.cursor()
    cur.execute('CREATE TABLE t (test)')
    low, high = 0, 100000
    while (high - 1) > low:
        guess = (high + low) // 2
        query = 'INSERT INTO t VALUES ' + ','.join(['(?)' for _ in
                                                    range(guess)])
        args = [str(i) for i in range(guess)]
        try:
            cur.execute(query, args)
        except sqlite3.OperationalError as e:
            if "too many SQL variables" in str(e):
                high = guess
            else:
                raise
        else:
            low = guess
    cur.close()
    db.close()
    return low


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    # with the global statement line we make sure to change the global variable at the top
    # when settin gup the logger
    global logger
    logger = setup_logging(
        write_log_to_file=args.write_log_to_file,
        log_file_base=args.log_file_base,
        log_level_file=args.log_level_file,
        log_level=args.log_level,
        progress_bar=args.progress_bar
    )

    # max_variables_sql = max_sql_variables()
    # logger.info("Maximum variables sql: {}".format(max_sql_variables()))

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
