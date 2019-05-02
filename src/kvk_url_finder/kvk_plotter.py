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
    parser.add_argument("--type", default="all", choices=PLOT_TYPES, help="Choice a plot type")
    parser.add_argument("--user", action="store",
                        help="Username of the postgres database. By default use current user")
    parser.add_argument("--database", action="store", default="kvk_db",
                        help="Name of the database to plot")
    parser.add_argument("--password", action="store",
                        help="Password of the postgres database")
    parser.add_argument("--reset", action="store_true",
                        help="If true reset the cache and reread the data")
    parser.add_argument("--dump_to_file", action="store_true",
                        help="If true dump the sql table to csv file")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")
    parser.add_argument("--input_file", action="store", help="Name of the input excel file")
    parser.add_argument("--all_cores", action="store_true", help="Include all the cores in the "
                                                                 "process time plot")
    parser.add_argument("--last_date", action="store", help="Last date of fit. Default last time")
    parser.add_argument("--save_image", action="store_true", help="If true, the image is saved to file")
    parser.add_argument("--show_image", action="store_true", help="If true, show the image", default=True)
    parser.add_argument("--noshow_image", action="store_false", help="If true, do not show the image", 
            dest="show_image")
    parser.add_argument("--image_type", action="store", help="Type of the image", default="png")
    parser.add_argument("--number_of_points", action="store", type=int,
                        help="Number of points to use for fit", default=1000)

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


class KvkPlotter(object):
    def __init__(self, database="kvk_db", user="evlt", password=None, hostname="localhost",
                 reset=False, input_file=None, dump_to_file=False, all_cores=False,
                 n_point_from_end=1000, last_date=None, save_image=False, image_type="png"
                 ):

        self.reset = reset
        self.all_cores = all_cores
        self.n_point_from_end = n_point_from_end
        self.last_date = last_date
        self.save_image = save_image
        self.image_type = image_type
        self.dump_to_file = dump_to_file
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

        if self.dump_to_file:
            df.to_csv(table_name + ".csv")

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
        ax = df.plot(y=["url_rank", "url_rank2"], style=".")

        if self.save_image:
            ax.save_image("."join([table_name, self.image_type]))


    def read_input_file(self):

        # set keep_default_na to false other the NA string by Arjen are replaced with nan
        df = pd.read_excel(self.input_file, keep_default_na=False, na_values="")
        df = df.rename(columns={KVK_KEY_2: KVK_KEY})

        df = df[~(df["Zelf"] == "NA")]

        # is_nummeric = df[KVK_KEY].astype(str).str.isdigit()
        # df = df[is_nummeric]
        # df.drop_duplicates([KVK_KEY], inplace=True)
        df.set_index(KVK_KEY, inplace=True, drop=True)

        return df

    @staticmethod
    def make_plot(df, ax1, bar_color="tab:blue", line_color="tab:red", add_ticks=True):

        count = pd.value_counts(df["ranking"]).sort_index()
        count.index = count.index.astype(int)
        ax1.set_ylabel("% kvks")

        tot = count.sum()

        count = 100 * (count / tot)
        count.plot(kind="bar", ax=ax1, label="# kvks", rot=0, color=bar_color)

        ax2 = ax1.twinx()
        if add_ticks:
            ax2.set_ylabel("cumulative %")

        ax2.set_ylim([0, 100])

        cum_sum = count.cumsum()
        cum_sum.plot(ax=ax2, style="-o", color=line_color, label="cdf")
        if add_ticks:
            ax2.tick_params(axis="y", labelcolor=line_color)

    def plot_company_ranking(self):

        # df sel contains the data of the subset of arjen
        df_sel = self.read_input_file()

        table_name = 'company'
        data_df = read_sql_table(table_name, connection=self.connection, reset=self.reset)
        # df[df["datetime"].isnull]
        data_df.dropna(axis=0, subset=["datetime"], inplace=True)

        data_df.set_index(KVK_KEY, inplace=True, drop=True)

        if self.dump_to_file:
            data_df.to_csv(table_name + ".csv")

        df_sel = pd.concat([data_df, df_sel], axis=1, join="inner")

        count_sel = pd.value_counts(df_sel["ranking"]).sort_index()
        count_sel.index = count_sel.index.astype(int)
        tot_sel = count_sel.sum()
        count_sel = 100 * (count_sel / tot_sel)
        print("counted sel {}".format(tot_sel))

        count_all = pd.value_counts(data_df["ranking"]).sort_index()
        count_all.index = count_all.index.astype(int)
        tot_all = count_all.sum()
        count_all = 100 * (count_all / tot_all)
        print("counted all {}".format(tot_all))

        count_all = pd.concat([count_all, count_sel], axis=1)

        count_all.columns = [f"All (N={tot_all})",
                             f"Sel (N={tot_sel}"]

        fig, axis = plt.subplots(figsize=(7.5, 5))
        plt.subplots_adjust(left=0.1, right=0.75)
        axis.set_xlabel("Ranking [-]")
        axis.set_ylim([0, 25])
        axis.set_ylabel("% kvks")

        count_all.plot(kind="bar", ax=axis, label="# kvks", rot=0)
        axis.set_xlim([-1, 10])

        ax2 = axis.twinx()
        ax2.set_ylabel("cumulative %")

        cum_sum_all = count_all.cumsum()
        cum_sum_sel = pd.DataFrame(index=count_sel.index, data=count_sel.cumsum().values,
                                   columns=[count_all.columns[1]])
        #
        cum_sum_all.plot(y=[cum_sum_all.columns[0]], ax=ax2, style="--o", color="tab:red",
                         legend=False)
        cum_sum_sel.plot(y=[cum_sum_sel.columns[0]], ax=ax2, style="--x", color="tab:green",
                         legend=False)

        ax2.set_ylim([0, 110])
        ax2.set_xlim([-1, 10])
        #
        ax2.tick_params(axis="y", labelcolor="black")
        axis.legend(bbox_to_anchor=(1.39, 1.05), title="KVK")
        ax2.legend(bbox_to_anchor=(1.39, 0.85), title="Cumulative")

        plt.savefig("url_score_DH.jpg")

    #
    # self.make_plot(data_df, axis)
    # self.make_plot(df_sel, axis, bar_color="tab:green", line_color="tab:orange",
    #               add_ticks=False)

    def plot_processing_time(self):
        table_name = 'company'
        df = read_sql_table(table_name, connection=self.connection, reset=self.reset)
        df.dropna(axis=0, subset=["datetime"], inplace=True)
        df.sort_values(["datetime"], inplace=True)
        df.reset_index(inplace=True)
        df.set_index("datetime", drop=True, inplace=True)

        df["tot_count"] = range(1, len(df) + 1)
        df["datetime_num"] = pd.to_numeric(df.index)
        df["tot_predict"] = None

        if self.last_date is None:
            df_sel = df.tail(self.n_point_from_end).copy()
        else:
            last_date = df[df.index <= self.last_date]

        logger.info("Fitting line")
        fit = np.polyfit(df_sel["datetime_num"], df_sel["tot_count"], 1)
        comp_per_sec = fit[0] * 1e9
        sec_per_comp = 1 / comp_per_sec
        logger.info(f"Company scraping rate: {comp_per_sec} Comp/sec")
        logger.info(f"Time per company: {sec_per_comp} Sec/comp")

        poly = np.poly1d(fit)
        df_sel.loc[:, "tot_predict"] = poly(df_sel["datetime_num"])

        fig, ax = plt.subplots(figsize=(8, 8))
        line_labels = list()

        df.plot(y=["tot_count"], style="-", ax=ax)
        line_labels.append("total")
        df_sel.plot(y=["tot_predict"], style="--", ax=ax)
        line_labels.append("predict")

        if self.all_cores:
            for core_id, core_df in df.groupby("core_id"):
                logger.info(f"plotting core {core_id}")
                core_df["count"] = range(1, len(core_df) + 1)
                core_df.plot(y=["count"], style="-", ax=ax)
                line_labels.append(int(core_id))

        ax.legend(line_labels, title="Core")

        if self.save_image:
            fig.save_image("."join([table_name, self.image_type]))


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    assert args.type in PLOT_TYPES

    kvk_plotter = KvkPlotter(database=args.database,
                             user=args.user,
                             password=args.password,
                             hostname=args.hostname,
                             reset=args.reset,
                             input_file=args.input_file,
                             dump_to_file=args.dump_to_file,
                             all_cores=args.all_cores,
                             n_point_from_end=args.number_of_points,
                             last_date=args.last_date,
                             save_image=args.save_image,
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

    if args.show_image:
        plt.show()


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
