import argparse
import logging
import os
import sys

import pandas as pd

from cbs_utils.misc import (create_logger)

try:
    from cbs_tools import __version__
except ImportError:
    __version__ = "unknown"

__author__ = "Eelco van Vliet"
__copyright__ = "Eelco van Vliet"
__license__ = "mit"

# set up global logger
LOGGER_NAME = os.path.basename(__file__)

_logger = create_logger(name=LOGGER_NAME, console_log_format_clean=True)


class CsvFileTranscripter(object):
    """
    Class to parse a csv file and transcript the columns

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

    def __init__(self, input_file_name):
        self.input_file_name = input_file_name

        _logger.info("Transcripting {}".format(input_file_name))


def _parse_the_command_line_arguments(args):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # parse the command line to set some options2
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser(description='Parse a CSV file and transcript the columns',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # set the verbosity level command line arguments
    # mandatory arguments
    parser.add_argument("input_file_name", action="store",
                        help="The CSV file to transcript")
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

    CsvFileTranscripter(
        input_file_name=args.input_file_name,
    )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
