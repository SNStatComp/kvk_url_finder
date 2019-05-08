import argparse
import logging
import os
import sys
from pathlib import Path
import numpy as np
import statsmodels.api as sm

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

from kvk_url_finder.utils import read_sql_table

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PLOT_TYPES = ["process_time", "web_ranking", "all", "company_ranking"]
KVK_KEY_2 = "NhrVestKvkNummer"
KVK_KEY = "kvk_nummer"


def _parse_the_command_line_arguments(args):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # parse the command line to set some options2
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser(description='Make plots of the kvk url data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # set the verbosity level command line arguments
    # mandatory arguments
    parser.add_argument("--version", help="Show the current version", action="version",
                        version="{}\nPart of kvk_url_finder version {}".format(
                            os.path.basename(__file__), __version__))
    parser.add_argument('-d', '--debug', help="Print lots of debugging statements",
                        action="store_const", dest="log_level", const=logging.DEBUG,
                        default=logging.INFO)
    parser.add_argument('-v', '--verbose', help="Be verbose", action="store_const",
                        dest="log_level", const=logging.INFO)
    parser.add_argument('-q', '--quiet', help="Be quiet: no output", action="store_const",
                        dest="log_level", const=logging.WARNING)
    parser.add_argument("--name", default="kvk_url_finder")
    parser.add_argument("--user", default="evlt")

    # parse the command line
    parsed_arguments = parser.parse_args(args)


def main(args_in):

    args, parser = _parse_the_command_line_arguments(args_in)

    user = args.user
    name = args.name

    logger.info(f"Give memory usage of {user} of job {name}")







def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()

