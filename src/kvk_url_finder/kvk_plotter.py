import argparse
import logging
import os
import sys
from pathlib import Path

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
KVK_KEY = "NhrVestKvkNummer"


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
    parser.add_argument("--reset", action="store_true",
                        help="If true reset the cache and reread the data")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")
    parser.add_argument("--input_file", action="store", help="Name of the input excel file")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


class KvkPlotter(object):
    def __init__(self, database="kvk_db", user="evlt", password=None, hostname="localhost",
                 reset=False, input_file=None):

        if input_file is not None:
            self.input_file = Path(input_file)
        else:
            self.input_file = None

        logger.info(f"Opening database {database} for user {user} at {hostname}")
        self.connection = psycopg2.connect(host=hostname, database=database, user=user,
                                           password=password)

    def plot_website_ranking(self):
        table_name = 'web_site'
        df = read_sql_table(table_name, connection=self.connection, reset=self.reset)

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

    def read_input_file(self):

        # set keep_default_na to false other the NA string by Arjen are replaced with nan
        df = pd.read_excel(self.input_file, keep_default_na=False, na_values="")
        is_nummeric = df[KVK_KEY].astype(str).str.isdigit()
        df = df[is_nummeric]
        df.drop_duplicates([KVK_KEY], inplace=True)
        df.set_index(KVK_KEY, inplace=True, drop=True)

        return df

    def plot_company_ranking(self):

        df_sel = self.read_input_file()
        df_sel.info()

        table_name = 'company'
        df = read_sql_table(table_name, connection=self.connection, reset=self.reset)
        # df[df["datetime"].isnull]
        df.dropna(axis=0, subset=["datetime"], inplace=True)

        df.set_axis()

        count = pd.value_counts(df["ranking"]).sort_index()
        count.index = count.index.astype(int)
        print(count)

        tot = count.sum()

        count = 100 * (count / tot)

        fig, axis = plt.subplots()
        axis.set_xlabel("Ranking [-]")
        axis.set_ylabel("% kvks")

        count.plot(kind="bar", ax=axis, label="# kvks", rot=0)

        color = "tab:red"
        ax2 = axis.twinx()
        ax2.set_ylabel("cumulative %", color=color)

        cum_sum = count.cumsum()
        cum_sum.plot(ax=ax2, style="-o", color=color, label="cdf")
        ax2.tick_params(axis="y", labelcolor=color)

        # fig.tight_layout()

        #lines = axis.get_lines() + ax2.get_lines()
        #axis.legend(lines, [line.get_label() for line in lines], loc='top right')


    def plot_processing_time(self):
        table_name = 'company'
        df = read_sql_table(table_name, connection=self.connection, reset=self.reset)
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


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    assert args.type in PLOT_TYPES

    kvk_plotter = KvkPlotter(database=args.database,
                             user=args.user,
                             password=args.password,
                             hostname=args.hostname,
                             reset=args.reset,
                             input_file=args.input_file
                             )

    if args.type in ("process_time", "all"):
        logger.info("Plotting process time")
        kvk_plotter.plot_processing_time()
    if args.type in ("web_ranking", "all"):
        logger.info("Plotting web ranking")
        kvk_plotter.plot_website_ranking()
    if args.type in ("company_ranking", "all"):
        logger.info("Plotting company ranking")
        kvk_plotter.plot_company_ranking()

    plt.show()


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
