import argparse
import logging
import os
import sys
import tables
import time

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

CACHE_TYPES = ["msg_pack", "hdf", "sql", "csv"]
COMPRESSION_TYPES = [None, "zlib", "blosc"]

logging.basicConfig()
_logger = logging.getLogger(__name__)

# set up progress bar properties
PB_WIDGETS = [pb.Percentage(), ' ', pb.Bar(marker='.', left='[', right=']'), ""]


class Timer(object):
    """Class to measure the time it takes execute a section of code

    Parameters
    ----------
    message : str
        a string to use to the output line
    name : str, optional
        The name of the routine timed.
    verbose : bool, optional
        if True, produce output
    units : str, optional
        time units to use. Default  'ms'
    n_digits : int, optional
        number of decimals to add to the timer units

    Example
    -------

    Use a `with` / `as` construction to enclose the section of code which need to be timed

    >>> from numpy import allclose
    >>> number_of_seconds = 1.0
    >>> with Timer(units="s", n_digits=0) as timer:
    ...    time.sleep(number_of_seconds)
    Elapsed time         routine              :          1 s
    >>> allclose(number_of_seconds, timer.secs, rtol=0.1)
    True
    """

    def __init__(self, message="Elapsed time", name="routine", verbose=True, units='ms', n_digits=0,
                 field_width=20):
        self.logger = _logger
        self.verbose = verbose
        self.message = message
        self.name = name
        self.units = units
        self.secs = None

        # build the format string. E.g. for field_with=20 and n_digits=1 and units=ms, this produces
        # the following
        # "{:<20s} : {:<20s} {:>10.1f} ms"
        self.format_string = "{:<" + \
                             "{}".format(field_width) + \
                             "s}" + \
                             " {:<" + \
                             "{}".format(field_width) + \
                             "s} : {:>" + "{}.{}".format(10, n_digits) + \
                             "f}" + \
                             " {}".format(self.units)

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()

        # start and end are in seconds. Convert time delta to nano seconds
        self.delta_time = np.timedelta64(int(1e9 * (self.end - self.start)), 'ns')

        self.secs = float(self.delta_time / np.timedelta64(1, "s"))

        # debug output
        self.logger.debug("Found delta time in ns: {}".format(self.delta_time))

        if self.verbose:
            # convert the delta time to the desired units
            duration = self.delta_time / np.timedelta64(1, self.units)

            # produce output
            self.logger.info(self.format_string.format(self.message, self.name, duration,
                                                       self.units))


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
    maximum_entries: int
        Give the maximum number of entries to process. Default = None, which means all entries are used
    """

    def __init__(self, input_file_name, reset_database=False, cache_type=None, compression=None,
                 maximum_entries=None):

        self.input_file_name = input_file_name
        self.cache_type = cache_type
        if cache_type == "msg_pack":
            self.output_file_name = os.path.splitext(input_file_name)[0] + ".msg_pack"
        elif cache_type == "hdf":
            self.output_file_name = os.path.splitext(input_file_name)[0] + ".h5"
        elif cache_type == "sql":
            self.output_file_name = os.path.splitext(input_file_name)[0] + ".sql"
        elif cache_type == "csv":
            self.output_file_name = os.path.splitext(input_file_name)[0] + "_new.csv"
        else:
            raise AssertionError("Cache type can only one of the following: {}. Found {}"
                                 "".format(CACHE_TYPES, cache_type))
        self.reset_database = reset_database

        self.maximum_entries = maximum_entries

        self.compression = compression

        if self.output_file_name == self.input_file_name:
            raise AssertionError("Data base file name equal to input file name. Please at a proper"
                                 "extension to your input file ")

        self.data = None  # contains the pandas data frame

        # read from either original csv or cache. After this the data attribute is filled with a data frame
        self.read_database()

        _logger.info("Read Data Frame info\n{}".format(self.data.info))
        _logger.info("Read Data Frame head\n{}".format(self.data.head()))

    def read_database(self):
        """
        Read the URL data base
        """

        if self.reset_database or not os.path.exists(self.output_file_name):
            # we are running the script for the first time or we want to reset the cache, so
            # read the original csv data and store it to cache
            _logger.info("Reading data from original data base {name}"
                         "".format(name=self.input_file_name))
            with Timer(name="read_csv") as _:
                self.data = pd.read_csv(self.input_file_name, header=None, usecols=[1, 2, 4],
                                        names=["KvK", "Name", "URL"], nrows=self.maximum_entries)

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
            with Timer(name="read cache") as _:
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
            self.data.to_msgpack(self.output_file_name)
        elif self.cache_type == "hdf":
            self.data.to_hdf(self.output_file_name, key="kvk_data", mode="w", dropna=True,
                             format="fixed")
        elif self.cache_type == "sql":
            self.data.to_sql(self.output_file_name)
        elif self.cache_type == "csv":
            self.data.to_csv(self.output_file_name)
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
        elif self.cache_type == "sql":
            self.data = pd.read_sql(self.output_file_name)
        elif self.cache_type == "csv":
            self.data = pd.read_sql(self.output_file_name)
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
    parser.add_argument('--compression', help="Type of the compression ",
                        choices=COMPRESSION_TYPES, default="zlib")
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
        input_file_name=args.input_file_name,
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
