import argparse
import logging
import os
import sys

import peewee as pw
import pandas as pd
import progressbar as pb

from cbs_utils.misc import (Timer, create_logger)

try:
    from cbs_tools import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

CACHE_TYPES = ["msg_pack", "hdf", "sql", "csv", "pkl"]
COMPRESSION_TYPES = [None, "zlib", "blosc"]

# set up global logger
LOGGER_NAME = os.path.basename(__file__)

logger = create_logger(name=LOGGER_NAME, console_log_format_clean=True)

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
class FinalKvkregister(BaseModel):
    id = pw.IntegerField(null=True)
    crawldate = pw.DateTimeField(null=True)
    handelsnaam = pw.CharField(null=True)
    kvknr = pw.CharField(primary_key=True)
    plaats = pw.CharField(null=True)
    postcode = pw.CharField(null=True)
    straat = pw.CharField(null=True)

    class Meta:
        table_name = 'final_kvkregister'


class KvkUrl(BaseModel):
    id = pw.AutoField(primary_key=True)
    kvknr = pw.CharField(null=False)
    handelsnaam = pw.CharField(null=True)
    url = pw.CharField(null=False)


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
                 ):

        logger.info("Connecting to database {}".format(database_name))
        database.init(database_name)
        database.connect()

        # make table connections
        self.kvk_key = kvk_key
        self.name_key = name_key
        self.url_key = url_key
        self.kvk_register = FinalKvkregister()
        self.kvk_url_scrape_result = KvkUrl()

        self.url_input_file_name = url_input_file_name
        self.reset_database = reset_database

        self.maximum_entries = maximum_entries

        self.compression = compression
        self.progressbar = progressbar

        self.data = pd.DataFrame()  # contains the pandas data frame

        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.read_database()

        logger.debug("Read Data Frame info\n{}".format(self.data.info))
        logger.debug("Read Data Frame head\n{}".format(self.data.head()))

        self.make_report()

    def make_report(self):
        """
        Report all the tables and data we have loaded so far
        """
        _logger = logging.getLogger(LOGGER_NAME)
        number_of_kvk_companies = self.kvk_register.select().count()
        _logger.info("Head of all {} kvk entries".format(number_of_kvk_companies))
        for cnt, record in enumerate(self.kvk_register.select().paginate(0)):
            _logger.info("{:03d}: {} - {}".format(cnt, record.kvknr, record.handelsnaam))

    def read_database(self):
        """
        Read the URL data base
        """
        _logger = logging.getLogger(LOGGER_NAME)

        file_base, file_ext = os.path.splitext(self.url_input_file_name)

        if self.reset_database or not self.kvk_url_scrape_result.table_exists():
            # we are running the script for the first time or we want to reset the cache, so
            # read the original csv data and store it to cache
            _logger.info("Reading data from original data base {name}"
                         "".format(name=self.url_input_file_name))
            with Timer(name="read url") as _:

                if file_ext == ".csv":
                    self.data = pd.read_csv(self.url_input_file_name,
                                            header=None,
                                            usecols=[1, 2, 4],
                                            names=["kvknr", "handelsnaam", "url"],
                                            nrows=self.maximum_entries)
                else:
                    self.data = pd.read_hdf(self.url_input_file_name, stop=self.maximum_entries)
                    self.data.reset_index(inplace=True)

            with Timer(name="dump_kvk_url_to_mysql") as _:
                self.dump_kvk_url_to_myqsl()
        else:
            # we have read the csv file before so now we can read from the cache
            with Timer(name="read cache") as _:
                self.read_database_from_cache()

        _logger.info("Done reading file")
        _logger.debug("Data info")
        _logger.debug("Data head")

    def dump_kvk_url_to_myqsl(self):
        """
        Dump the original list to mysql
        """
        _logger = logging.getLogger(LOGGER_NAME)
        _logger.info("Start writing to mysql data base")

        database.drop_tables([KvkUrl])
        database.create_tables([KvkUrl])

        number_of_rows = self.data.index.size
        progress = None

        for cnt, (index, row) in enumerate(self.data.iterrows()):
            kvknr = row[self.kvk_key]
            handels_naam = row[self.name_key]
            url = row[self.url_key]
            if self.progressbar:
                PB_WIDGETS[-1] = PB_MESSAGE_FORMAT.format(cnt + 1, number_of_rows)
                if progress is None:
                    progress = pb.ProgressBar(widgets=PB_WIDGETS, maxval=number_of_rows,
                                              fd=sys.stdout).start()
                progress.update(cnt + 1)
                sys.stdout.flush()
            _logger.debug("Storing query {:06d}: {:7d} - {:20s} - {}".format(
                index, kvknr, handels_naam, url))

            # create the query in the table
            KvkUrl.create(kvknr=kvknr, handelsnaam=handels_naam, url=url)

        if progress:
            progress.finish()

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

        _logger = logging.getLogger(LOGGER_NAME)
        _logger.info("Reading data from cached file {name}".format(name=self.cache_url_file_name))
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

    _logger = create_logger(name=LOGGER_NAME, console_log_format_clean=True,
                            console_log_level=args.log_level)

    script_name = os.path.basename(sys.argv[0])
    start_time = pd.to_datetime("now")
    _logger.info("Start {script} (v: {version}) at {start_time}:{cmd}".format(script=script_name,
                                                                              version=__version__,
                                                                              start_time=start_time,
                                                                              cmd=sys.argv[:]))
    # change the log level to our requested level
    if args.progressbar:
        _logger.setLevel(logging.INFO)

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
