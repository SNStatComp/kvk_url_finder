import argparse
import logging
import os
import re
import sys

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

    def __init__(self, input_file_name, reset_database=False, compress_format=None):

        self.input_file_name = input_file_name
        self.msg_pack_file_name = os.path.splitext(input_file_name)[0] + ".msg_pack"
        self.reset_database = reset_database

        self.compress_format = compress_format

        if self.msg_pack_file_name == self.input_file_name:
            raise AssertionError("Data base file name equal to input file name. Please at a proper"
                                 "extension to your input file ")

        self.data = None  # contains the pandas data frame

        self.read_database()

    def read_database(self):
        """
        Read the URL data base
        """

        if self.reset_database or not os.path.exists(self.msg_pack_file_name):
            _logger.info("Reading data from original data base {name}"
                         "".format(name=self.input_file_name))
            self.data = pd.read_csv(self.input_file_name, header=None, usecols=[1, 2, 4],
                                    names=["KvK", "Name", "URL"])

            _logger.debug("Read\n{}".format(self.data.head()))
            _logger.debug("Read\n{}".format(self.data.info))

            _logger.info("Dumping data to cache file {} ".format(self.msg_pack_file_name))
            pd.to_msgpack(self.msg_pack_file_name, self.data)
        else:
            _logger.info("Reading data from cached file {name}"
                         "".format(name=self.msg_pack_file_name))
            self.data = pd.read_msgpack(self.msg_pack_file_name)

        _logger.info("Done reading file")
        _logger.debug("Data info \n{}".format(self.data.info))


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
                                                 "a message pack file already", action="store_true")

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
    )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
