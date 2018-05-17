import argparse
import logging
import os
import sys

import peewee as pw
import pandas as pd
import progressbar as pb

from cbs_utils.misc import Timer

try:
    from cbs_tools import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

CACHE_TYPES = ["msg_pack", "hdf", "sql", "csv", "pkl"]
COMPRESSION_TYPES = [None, "zlib", "blosc"]

logging.basicConfig()
_logger = logging.getLogger(__name__)

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]

_database = pw.MySQLDatabase('kvk_db',
                             **{'charset': 'utf8', 'use_unicode': True, 'user': 'root', 'password': 'vliet123'})


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(pw.Model):
    class Meta:
        _database = _database


class FinalKvkregister(BaseModel):
    id = pw.AutoField(column_name='ID')
    crawldate = pw.DateTimeField(null=True)
    handelsnaam = pw.CharField(null=True)
    kvknr = pw.CharField()
    plaats = pw.CharField(null=True)
    postcode = pw.CharField(null=True)
    straat = pw.CharField(null=True)

    class Meta:
        table_name = 'final_kvkregister'


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

    def __init__(self, url_input_file_name, reset_database=False, cache_type=None, compression=None,
                 maximum_entries=None,
                 database_name="kvk_db", database_user="root", database_password="vliet123"):

        _logger.info("Connecting to database {}".format(database_name))
        _database.init(database_name, **{'charset': 'utf8',
                                        'use_unicode': True,
                                        'user': database_user,
                                        'password': database_password
                                         }
                       )
        _database.connect()

        self.url_input_file_name = url_input_file_name
        url_file_base_name = os.path.splitext(url_input_file_name)[0]
        self.cache_type = cache_type
        if cache_type == "msg_pack":
            self.cache_url_file_name = url_file_base_name + ".msg_pack"
        elif cache_type == "hdf":
            self.cache_url_file_name = url_file_base_name + ".h5"
        elif cache_type == "sql":
            self.cache_url_file_name = url_file_base_name + ".sql"
        elif cache_type == "csv":
            self.cache_url_file_name = url_file_base_name + "_new.csv"
        elif cache_type == "pkl":
            self.cache_url_file_name = url_file_base_name + ".pkl"
        else:
            raise AssertionError("Cache type can only one of the following: {}. Found {}"
                                 "".format(CACHE_TYPES, cache_type))
        self.reset_database = reset_database

        self.maximum_entries = maximum_entries

        self.compression = compression

        if self.cache_type == "pkl" and self.compression == "zlib":
            _logger.warning("compression set to zlib but pkl cache is selected. Changing "
                            "compression to zip")
            self.compression = "zip"

        if self.cache_url_file_name == self.url_input_file_name:
            raise AssertionError("Data base file name equal to input file name. Please at a proper"
                                 "extension to your input file ")

        self.data = None  # contains the pandas data frame

        # read from either original csv or cache. After this the data attribute is filled with a
        # data frame
        self.read_database()

        _logger.debug("Read Data Frame info\n{}".format(self.data.info))
        _logger.debug("Read Data Frame head\n{}".format(self.data.head()))

    def read_database(self):
        """
        Read the URL data base
        """

        if self.reset_database or not os.path.exists(self.cache_url_file_name):
            # we are running the script for the first time or we want to reset the cache, so
            # read the original csv data and store it to cache
            _logger.info("Reading data from original data base {name}"
                         "".format(name=self.url_input_file_name))
            with Timer(name="read_csv") as _:
                self.data = pd.read_csv(self.url_input_file_name, header=None, usecols=[1, 2, 4],
                                        names=["KvK", "Name", "URL"], nrows=self.maximum_entries)

            # clean the data base a bit and set the kvk number as the index of the database
            self.data.fillna("", inplace=True)

            # for hdf 5 we need to explicitly convert the data to strings
            for column_name in self.data.columns:
                _logger.debug("Converting {}".format(column_name))
                self.data[column_name] = self.data[column_name].astype(str)

            # store the data to cache which we can read the next run
            with Timer("store data") as _:
                self.store_database_to_cache()
        else:
            # we have read the csv file before so now we can read from the cache
            with Timer(name="read cache") as _:
                self.read_database_from_cache()

        _logger.info("Done reading file")
        _logger.debug("Data info")
        _logger.debug("Data head")

    def store_database_to_cache(self):
        """
        Store the data base to the cache file. Depending on the *cache_type* atttribute this can be
        either message pack or hdf 5 format
        """

        _logger.info("Dumping data to cache file {} ".format(self.cache_url_file_name))
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

    # change the log level to our requested level
    if not args.progressbar:
        _logger.setLevel(args.log_level)
    else:
        _logger.setLevel(logging.CRITICAL)

    script_name = os.path.basename(sys.argv[0])
    start_time = pd.to_datetime("now")
    _logger.info("Start {script} (v: {version}) at {start_time}:{cmd}".format(script=script_name,
                                                                              version=__version__,
                                                                              start_time=start_time,
                                                                              cmd=sys.argv[:]))
    KvKUrlParser(
        url_input_file_name=args.url_input_file_name,
        reset_database=args.reset_database,
        cache_type=args.cache_type,
        compression=args.compression,
        maximum_entries=args.maximum_entries,
    )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
