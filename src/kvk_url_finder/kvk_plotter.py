import argparse
import logging
import os
import sys

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
    parser.add_argument("--type", default="all", choices=PLOT_TYPES, help="Choice a plot type")
    parser.add_argument("--user", action="store",
                        help="Username of the postgres database. By default use current user")
    parser.add_argument("--database", action="store", default="kvk_db",
                        help="Name of the database to plot")
    parser.add_argument("--password", action="store",
                        help="Password of the postgres database")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


class KvkPlotter(object):
    def __init__(self, database="kvk_db", user="evlt", password=None, hostname="localhost"):

        logger.info(f"Opening database {database} for user {user} at {hostname}")
        self.connection = psycopg2.connect(host=hostname, database=database, user=user,
                                           password=password)

    def read_table(self, table_name):
        cache_file = table_name + ".pkl"
        try:
            df = pd.read_pickle(cache_file)
            logger.info(f"Read table pickle file {cache_file}")
        except IOError:
            logger.info("Connecting to database")
            logger.info(f"Start reading table from postgres table {table_name}.pkl")
            df = pd.read_sql(f"select * from {table_name}", con=self.connection)
            logger.info("Dumping to pickle file")
            df.to_pickle(cache_file)

        return df

    def plot_website_ranking(self):
        table_name = 'web_site'
        df = self.read_table(table_name)

        df["url_match"] = df.levenshtein * (1 - df.string_match)
        max_match = 10
        max_score = 3
        df["url_rank"] = max_score * (1 - df.url_match / max_match)
        df["url_rank2"] = max_score * (1 - df.levenshtein / max_match)
        df.info()

        df2 = df.set_index("company_id", drop=True)
        df2 = df2[
            ["naam", "url_id", "levenshtein", "string_match", "ranking", "url_rank", "url_rank2"]]
        df2.dropna(how="any", axis=0, inplace=True)

        logger.debug("don")

        # df.plot.scatter(x="levenshtein", y="url_rank", c="blue")
        # df.plot.scatter(x="levenshtein", y="url_rank2", c="red")
        # df.plot.scatter(x="url_rank", y="url_rank2", c="green")
        df.plot(y=["url_rank", "url_rank2"], style=".")

        plt.show()

    def plot_processing_time(self):
        table_name = 'company'
        df: pd.DataFrame = self.read_table(table_name)
        # df[df["datetime"].isnull]
        df.dropna(axis=0, subset=["datetime"], inplace=True)
        df.sort_values(["datetime"], inplace=True)
        df["delta_t"] = (df["datetime"] - df["datetime"].min()) / pd.to_timedelta(1, "s")
        df.reset_index(inplace=True)
        df.set_index("datetime", drop=True, inplace=True)

        df["tot_count"] = range(1, len(df) + 1)

        fig, ax = plt.subplots(figsize=(10, 12))
        line_labels = list()

        df.plot(y=["tot_count"], style="-", ax=ax)
        line_labels.append("total")

        for core_id, core_df in df.groupby("core_id"):
            logger.info(f"plotting core {core_id}")
            core_df["count"] = range(1, len(core_df) + 1)
            core_df.plot(y=["count"], style="-", ax=ax)
            line_labels.append(int(core_id))

        ax.legend(line_labels, title="Core")
        plt.show()


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    assert args.type in PLOT_TYPES

    kvk_plotter = KvkPlotter(database=args.database,
                             user=args.user,
                             password=args.password,
                             hostname=args.hostname,
                             )

    if args.type in ("process_time", "all"):
        logger.info("Plotting process time")
        kvk_plotter.plot_processing_time()
    if args.type in ("web_ranking", "all"):
        logger.info("Plotting web ranking")
        kvk_plotter.plot_website_ranking()


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
