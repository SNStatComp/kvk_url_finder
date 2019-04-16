import argparse
import logging
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PLOT_TYPES = ["process_time", "web_ranking", "all"]


def _parse_the_command_line_arguments(args):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # parse the command line to set some options2
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser(description='Validate a list of kvk numbers data',
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
    parser.add_argument("--type", default="all", choices=PLOT_TYPES, help="Choice a plot type")
    parser.add_argument("--user", action="store",
                        help="Username of the postgres database. By default use current user")
    parser.add_argument("--database", action="store", default="kvk_db",
                        help="Name of the database to plot")
    parser.add_argument("--password", action="store",
                        help="Password of the postgres database")
    parser.add_argument("--reset", action="store_true",
                        help="If true reset the cache and reread the data")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")
    parser.add_argument("input_file", action="store",
                        help="Name of the input file with a row of kvk numbers to validate. ")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


class KvkValidator(object):
    def __init__(self, database="kvk_db", user="evlt", password=None, hostname="localhost",
                 reset=False, input_file=None,
                 check_col_key="Url_Eelco_new",
                 ranking_col_key="ranking",
                 kvk_col_key="NhrVestKvkNummer"
                 ):

        self.check_col_key = check_col_key
        self.ranking_col_key = ranking_col_key
        self.kvk_col_key = kvk_col_key
        self.reset = reset
        self.input_file = Path(input_file)
        self.output_file = Path(self.input_file.stem + "_new.xls")
        logger.info(f"Opening database {database} for user {user} at {hostname}")
        self.connection = psycopg2.connect(host=hostname, database=database, user=user,
                                           password=password)

        self.kvk_in: pd.DataFrame = None
        self.kvb_db = None

        self.read_input_file()
        self.validate_kvk_numbers()

    def read_input_file(self):

        self.kvk_in = pd.read_excel(self.input_file)
        self.kvk_in.info()
        self.kvk_in[self.check_col_key] = None
        self.kvk_in[self.ranking_col_key] = None

    def read_table(self, table_name):
        cache_file = table_name + ".pkl"
        df = None
        if not self.reset:
            try:
                df = pd.read_pickle(cache_file)
                logger.info(f"Read table pickle file {cache_file}")
            except IOError:
                logger.debug(f"No pickle file available {cache_file}")

        if df is None:
            logger.info("Connecting to database")
            logger.info(f"Start reading table from postgres table {table_name}.pkl")
            df = pd.read_sql(f"select * from {table_name}", con=self.connection)
            logger.info(f"Dumping to pickle file {cache_file}")
            df.to_pickle(cache_file)
            logger.info("Done")

        return df

    def validate_kvk_numbers(self):

        table_name = 'company'
        df = self.read_table(table_name)
        df.set_index("kvk_nummer", drop=True, inplace=True)
        df.info()

        for index, row in self.kvk_in.iterrows():
            kvk_nr = row[self.kvk_col_key]
            logger.debug(f"found index {kvk_nr}")
            try:
                kvk_int = int(kvk_nr)
            except ValueError:
                continue

            logger.debug(f"Retrieve kvk {kvk_int}")

            try:
                url = df.loc[kvk_int, "url"]
                ranking = df.loc[kvk_int, "ranking"]
            except KeyError:
                logger.debug(f"Could not find {kvk_int} in my db")
                continue

            logger.debug(f"Storing url {url}")

            self.kvk_in.loc[index, self.check_col_key] = url
            self.kvk_in.loc[index, self.ranking_col_key] = ranking

        logger.info(f"Write result to  {self.output_file}")
        self.kvk_in.to_excel(self.output_file)


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    assert args.type in PLOT_TYPES

    kvk_check = KvkValidator(database=args.database,
                             user=args.user,
                             password=args.password,
                             hostname=args.hostname,
                             reset=args.reset,
                             input_file=args.input_file)


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
