import argparse
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from openpyxl import load_workbook

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PLOT_TYPES = ["process_time", "web_ranking", "all"]

KVK_KEY = "NhrVestKvkNummer"

INFO_COLS = ["CbsPersoonIdentificatie", "CpHandelsnaam", "Zelf"]
URL_COLS = ["URL_ABR", "URL_Dataprovider", "URL_Eelco", "URL_Dick_update", "URL_Eelco_update"]

RANKING_COLS = ["Url_Eelco_update"]


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
    parser.add_argument("--create", action="store_true",
                        help="Create the new url file. ")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


class KvkValidator(object):
    def __init__(self, database="kvk_db", user="evlt", password=None, hostname="localhost",
                 reset=False, input_file=None,
                 check_col_key="URL_Eelco_update",
                 ranking_col_key="ranking",
                 kvk_col_key="NhrVestKvkNummer",
                 create_new=False
                 ):

        self.check_col_key = check_col_key
        self.ranking_col_key = ranking_col_key
        self.kvk_col_key = kvk_col_key
        self.reset = reset
        self.input_file = Path(input_file)
        self.output_file = Path(self.input_file.stem + "_new.xlsx")
        logger.info(f"Opening database {database} for user {user} at {hostname}")
        self.connection = psycopg2.connect(host=hostname, database=database, user=user,
                                           password=password)

        self.kvk_in: pd.DataFrame = None
        self.kvb_db = None

        self.read_input_file()

        if create_new:
            self.create_new_excel()
        else:
            self.get_statistics()

    def read_input_file(self):

        # set keep_default_na to false other the NA string by Arjen are replaced with nan
        df = pd.read_excel(self.input_file, keep_default_na=False, na_values="")
        is_nummeric = df[KVK_KEY].astype(str).str.isdigit()
        df = df[is_nummeric]
        df.drop_duplicates([KVK_KEY], inplace=True)
        df.set_index(KVK_KEY, inplace=True, drop=True)
        df[INFO_COLS[0]] = df[INFO_COLS[0]].astype(int)

        self.kvk_in = pd.DataFrame(index=df.index, columns=INFO_COLS)
        for col in INFO_COLS:
            self.kvk_in[col] = df[col]

        for col in URL_COLS:
            try:
                self.kvk_in[col] = df[col]
            except KeyError:
                self.kvk_in[col] = None
            score_col = col + "_score"
            self.kvk_in[score_col] = 0
            if col in RANKING_COLS:
                ranking_col = col + "_rank"
                self.kvk_in[ranking_col] = 0

        self.kvk_in.info()

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

    def create_new_excel(self):

        table_name = 'company'
        df = self.read_table(table_name)
        df.set_index("kvk_nummer", drop=True, inplace=True)
        df.info()

        for kvk_nr, row in self.kvk_in.iterrows():

            logger.debug(f"Retrieve kvk {kvk_nr}")

            try:
                url = df.loc[kvk_nr, "url"]
                ranking = df.loc[kvk_nr, "ranking"]
            except KeyError:
                logger.debug(f"Could not find {kvk_nr} in my db")
                continue

            logger.debug(f"Storing url {url}")

            self.kvk_in.loc[kvk_nr, self.check_col_key] = url
            self.kvk_in.loc[kvk_nr, self.check_col_key + "_rank"] = ranking

        logger.info(f"Write result to  {self.output_file}")

        with pd.ExcelWriter(self.output_file, engine='xlsxwriter') as writer:
            self.kvk_in.to_excel(writer, sheet_name="input")

    def get_statistics(self):

        data_df = pd.read_excel(self.output_file, keep_default_na=False, na_values="")

        data_df = data_df[~(data_df["Zelf"]=="NA")]

        total = data_df.index.size

        STAT_COLS = [
            "n_total", "n_subtot", "n_none", "n_false", "n_good", "perc_good_of_total",
            "perc_good_of_found"]

        output_df = pd.DataFrame(index=STAT_COLS)

        def add_stat_cols(ds, stat_name, df_out):
            df_out[stat_name] = None
            total_sub = ds.size
            total_none = (ds == 0).sum()
            total_false = (ds == 1).sum()
            total_good = (ds == 2).sum()
            df_out.loc[STAT_COLS[0], stat_name] = total
            df_out.loc[STAT_COLS[1], stat_name] = total_sub
            df_out.loc[STAT_COLS[2], stat_name] = total_none
            df_out.loc[STAT_COLS[3], stat_name] = total_false
            df_out.loc[STAT_COLS[4], stat_name] = total_good
            df_out.loc[STAT_COLS[5], stat_name] = 100 * total_good / total
            df_out.loc[STAT_COLS[6], stat_name] = 100 * total_good / (total_false + total_good)

        for col in data_df.columns:
            if re.search("_score", col):
                logger.info(f"Calculating stats for column {col}")
                add_stat_cols(ds=data_df[col], stat_name=col, df_out=output_df)

                for min_rank in (1, 2, 3, 4, 5):
                    rank_col = re.sub("_score", "_rank", col)
                    if rank_col in data_df.columns:
                        rank_name = rank_col + f"{min_rank}"
                        logger.info(f"Also calculating filtered stat {rank_col} -> {rank_name}")
                        data_df_filtered = data_df[data_df[rank_col] >= min_rank]
                        add_stat_cols(ds=data_df_filtered[col], stat_name=rank_name,
                                      df_out=output_df)

        output_df = output_df.T

        logger.info(f"Appending statics to {self.output_file}")
        book = load_workbook(self.output_file)
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            writer.book = book
            writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

            output_df.to_excel(writer, 'Statistics')

            writer.save()


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    assert args.type in PLOT_TYPES

    kvk_check = KvkValidator(database=args.database,
                             user=args.user,
                             password=args.password,
                             hostname=args.hostname,
                             reset=args.reset,
                             input_file=args.input_file,
                             create_new=args.create
                             )


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
