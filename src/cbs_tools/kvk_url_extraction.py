import argparse
import logging
import os
import sys
import tables

import numpy as np
import pandas as pd
import progressbar as pb

try:
    from cbs_tools import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

CACHE_TYPES = ["msg_pack", "hdf"]

logging.basicConfig()
_logger = logging.getLogger(__name__)

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]


def progress_bar_message(cnt, total):
    return "Processed time {:d} of {:d}".format(cnt + 1, total)


class KvKUrlParser(object):
    """
    Class to parse a csv file and couple the unique kwk numbers to a list of urls

    Parameters
    ----------
    input_file_name: str
        Name of the input file
    reset_database: bool
        Reset the data base file in case this flag is True
    """

    def __init__(self, input_file_name, reset_database=False, compress_format=None,
                 cache_type=None):

        self.input_file_name = input_file_name
        self.cache_type = cache_type
        if cache_type == "msg_pack":
            self.output_file_name = os.path.splitext(input_file_name)[0] + ".msg_pack"
        elif cache_type == "hdf":
            self.output_file_name = os.path.splitext(input_file_name)[0] + ".h5"
        else:
            raise AssertionError("Cache type can only one of the following: {}. Found {}"
                                 "".format(CACHE_TYPES, cache_type))
        self.reset_database = reset_database

        self.compress_format = compress_format

        if self.output_file_name == self.input_file_name:
            raise AssertionError("Data base file name equal to input file name. Please at a proper"
                                 "extension to your input file ")

        self.data = None  # contains the pandas data frame

        self.read_database()

    def read_database(self):
        """
        Read the URL data base
        """

        if self.reset_database or not os.path.exists(self.output_file_name):
            # we are running the script for the first time or we want to reset the cache, so
            # read the original csv data and store it to cache
            _logger.info("Reading data from original data base {name}"
                         "".format(name=self.input_file_name))
            self.data = pd.read_csv(self.input_file_name, header=None, usecols=[1, 2, 4],
                                    names=["KvK", "Name", "URL"])

            # clean the data base a bit and set the kvk number as the index of the database
            self.data.fillna("", inplace=True)
            self.data.set_index(["KvK"], drop=True, inplace=True)

            # for hdf 5 we need to explicitly convert the data to strings
            for column_name in self.data.columns:
                _logger.debug("Converting {}".format(column_name))
                self.data[column_name] = self.data[column_name].astype(str)

            # store the data to cache which we can read the next run
            self.store_database_to_cache()
        else:
            # we have read the csv file before so now we can read from the cache
            self.read_database_from_cache()

        _logger.info("Done reading file")
        _logger.debug("Data info \n{}".format(self.data.info))
        _logger.debug("Data head \n{}".format(self.data.head()))

    def store_database_to_cache(self):
        """
        Store the data base to the cache file. Depending on the *cache_type* atttribute this can be
        either message pack or hdf 5 format
        """

        _logger.info("Dumping data to cache file {} ".format(self.output_file_name))
        if self.cache_type == "msg_pack":
            # dump to message pack
            self.data.to_msgpack(self.output_file_name, compress=self.compress_format)
        elif self.cache_type == "hdf":
            self.data.to_hdf(self.output_file_name, key="kvk_data", mode="w", dropna=True,
                             format="fixed")
        else:
            raise AssertionError("Invalid cache type found: {} ".format(self.cache_type))

    def read_database_from_cache(self):
        """
        Read the data base from the cache file
        """

        _logger.info("Reading data from cached file {name}".format(name=self.output_file_name))
        if self.cache_type == "msg_pack":
            self.data = pd.read_msgpack(self.output_file_name)
        elif self.cache_type == "hdf":
            self.data = pd.read_hdf(self.output_file_name)
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
    parser.add_argument("input_file_name", action="store",
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
                        choices=CACHE_TYPES, default="msg_pack")

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
    _logger.info("Start {script} (v: {version}) at {start_time}".format(script=script_name,
                                                                        version=__version__,
                                                                        start_time=start_time))
    KvKUrlParser(
        input_file_name=args.input_file_name,
        reset_database=args.reset_database,
        cache_type=args.cache_type,
    )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
