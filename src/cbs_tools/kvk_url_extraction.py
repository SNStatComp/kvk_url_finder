"""
Utility to import kvk/url combinations and turn it into a mysql data base

Usage:
    python kvk_url_extraction.py URL_kvk.csv.bz2  --max 10000

With --max you can limit the number of lines read from the csv file. In case the script is called
multiple times, you continue on last kvk you have stored in the sql database

The script can be runned with kernprof in order to time all the lines

kernprof -l kvk_url_extraction.py URL_kvk.csv.bz2  --max 10000


This generates a file kvk_url_extraction.py.prof


"""

import logging
import os
import sys

import peewee as pw
import pandas as pd
import progressbar as pb
import argparse


from cbs_utils.misc import (create_logger, merge_loggers)

try:
    from cbs_tools import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

try:
    # if profile exist, it means we are running kernprof to time all the lines of the functions
    # decorated with @profile
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
    kvk_nummer = pw.CharField(primary_key=True)
    naam = pw.CharField(null=True)
    plaats = pw.CharField(null=True)
    postcode = pw.CharField(null=True)
    straat = pw.CharField(null=True)


class WebSite(BaseModel):
    company = pw.ForeignKeyField(Company)
    kvk_nummer = pw.CharField(null=True)
    url = pw.CharField(null=False)
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
                 extend_database=False,
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

        # self.process_the_urls()

    @profile
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

    @profile
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

        if n_entries > 0:
            # in case we have already stored entries in the database, find the first n entrie for
            # which we can start reading
            n_entries = self.look_up_last_entry(n_entries)

        # we are running the script for the first time or we want to reset the cache, so
        # read the original csv data and store it to cache
        logger.info("Reading data from original data base {name}"
                    "".format(name=self.url_input_file_name))
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

        logger.info("Removing spurious urls")
        self.remove_spurious_urls()

        self.dump_kvk_url_to_myqsl()

    @profile
    def look_up_last_entry(self, n_entries):
        """
        In case we have N entries in the data base we want to continue reading in the csv file
        after 'at' least N entries. However, N could be larger, because we have removed url's before
        we wrote to the data base. This means that we can increase n. This is taken care of here
        """
        last_website = WebSite.select().order_by(WebSite.company_id.desc()).get()
        kvk_last = int(last_website.company.kvk_nummer)

        # df_all_all = pd.read_csv(self.url_input_file_name)
        tmp_data = pd.read_csv(self.url_input_file_name,
                               header=None,
                               usecols=[1, 2, 4],
                               names=[self.kvk_key, self.name_key, self.url_key],
                               nrows=self.maximum_entries,
                               skiprows=n_entries)

        try:
            row_index = tmp_data.loc[tmp_data[self.kvk_key] == kvk_last].index[-1]
        except IndexError:
            logger.debug("No last index found.  n_entries to skip to {}".format(n_entries))
        else:
            last_row = tmp_data.loc[row_index]
            logger.debug("found: {}".format(last_row))
            n_entries += row_index + 1
            logger.debug("Upated n_entries to skip to {}".format(n_entries))

        return n_entries

    @profile
    def remove_spurious_urls(self):
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

        self.data = urls.reset_index()

    @profile
    def remove_duplicated_entries(self):
        """
        Remove all the companies/url combination which already have been stored in
        the sql tables

        """

        # based on the data in the WebSite table create a data frame with all the kvk which
        # we have already included. These can be removed from the data we have just read
        nr = self.data.index.size
        logger.info("Removing duplicated kvk/url combinies. Data read at start: {}".format(nr))
        logger.debug("Getting all sql websides from database")
        kvk_list = list()
        url_list = list()
        name_list = list()
        for ws in WebSite.select():
            kvk_list.append(int(ws.kvk_nummer))
            url_list.append(ws.url)
            name_list.append(ws.naam)

        kvk_in_db = pd.DataFrame(
            data=list(zip(kvk_list, url_list, name_list)),
            columns=[KVK_KEY, URL_KEY, NAME_KEY])
        kvk_in_db.set_index([KVK_KEY, URL_KEY], drop=True, inplace=True)

        # drop all the kvk number which we already have loaded in the database
        logger.debug("Dropping all duplicated web sides")
        kvk_to_remove = self.data.set_index([KVK_KEY, URL_KEY])
        kvk_to_remove = kvk_to_remove.reindex(kvk_in_db.index)
        kvk_to_remove = kvk_to_remove[~kvk_to_remove[NAME_KEY].isnull()]
        try:
            self.data = self.data.set_index([KVK_KEY, URL_KEY]).drop(index=kvk_to_remove.index)
        except KeyError:
            logger.debug("Nothing to drop")
        else:
            self.data.reset_index(inplace=True)

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
        comp_df = self.data.set_index([KVK_KEY, URL_KEY])
        comp_df.drop(index=companies_in_db.index, level=0, inplace=True)
        self.data = comp_df.reset_index()

        nr = self.data.index.size
        logger.debug("Removed duplicated kvk/url combies. Data at end: {}".format(nr))


    @profile
    def dump_kvk_url_to_myqsl(self):
        """data
        Dump the original list to mysql
        """
        logger.info("Start writing to mysql data base")

        kvk = self.data[[KVK_KEY, NAME_KEY]].drop_duplicates([KVK_KEY])
        record_list = list(kvk.to_dict(orient="index").values())
        logger.info("Start writing table urls")

        n_batch = int(len(record_list) / MAX_SQL_CHUNK) + 1
        with database.atomic():
            for cnt, batch in enumerate(pw.chunked(record_list, MAX_SQL_CHUNK)):
                logger.info("Company chunk nr {}/{}".format(cnt + 1, n_batch))
                Company.insert_many(batch).execute()
        logger.debug("Done with company table")

        # create selection of data columns
        urls = self.data[[KVK_KEY, URL_KEY, NAME_KEY]]
        urls[COMPANY_KEY] = None
        urls.set_index([KVK_KEY, URL_KEY], inplace=True)

        # add a company key to all url and then make a reference to all companies from the Company
        # table
        logger.info("Adding companies to url table")
        company_vs_kvk = Company.select().where(Company.kvk_nummer << self.data[KVK_KEY].tolist())
        n_comp = len(company_vs_kvk)
        for counter, company in enumerate(company_vs_kvk):
            kvk_nr = int(company.kvk_nummer)
            urls.loc[[kvk_nr, ], COMPANY_KEY] = company
            if counter % MAX_SQL_CHUNK == 0:
                logger.info(" Added {} / {}".format(counter, n_comp))

        urls.reset_index(inplace=True)

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
                                                 "a sql file already", action="store_true")
    parser.add_argument('--extend_database', help="Extend the data base in case we have generated"
                                                  "a sql file already", action="store_true")
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


@profile
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
        extend_database=args.extend_database,
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
