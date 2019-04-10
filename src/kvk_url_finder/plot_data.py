import matplotlib.pyplot as plt
import pandas as pd
import logging
import psycopg2
import seaborn

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

CONNECTION = psycopg2.connect(database="kvk_db", user="evlt", password="vliet123")


def read_table(table_name):
    cache_file = table_name + ".pkl"
    try:
        df = pd.read_pickle(cache_file)
        logger.info(f"Read table pickle file {cache_file}")
    except IOError:
        logger.info("Connecting to database")
        logger.info(f"Start reading table from postgres table {table_name}.pkl")
        df = pd.read_sql(f"select * from {table_name}", con=CONNECTION)
        logger.info("Dumping to pickle file")
        df.to_pickle(cache_file)

    return df


def plot_processing_time():
    table_name = 'url_nl'
    df = read_table(table_name)
    #df[df["datetime"].isnull]
    df.dropna(axis=0, subset=["datetime"], inplace=True)
    df.sort_values(["datetime"], inplace=True)
    df["delta_t"] = (df["datetime"] - df["datetime"].min()) / pd.to_timedelta(1, "s")
    df.reset_index(inplace=True)
    df.set_index("datetime", drop=True, inplace=True)

    df["tot_count"] = df.count()
    df["count"] = df.groupby("group")


    #df.set_index(["datetime"], drop=True)
    df.info()
    df.plot(y=["index"], style=".")
    plt.show()


def plot_website_ranking():
    table_name = 'web_site'
    df = read_table(table_name)

    df["url_match"] = df.levenshtein * (1 - df.string_match)
    max_match = 10
    max_score = 3
    df["url_rank"] = max_score * (1 - df.url_match / max_match)
    df["url_rank2"] = max_score * (1 - df.levenshtein / max_match)
    df.info()

    df2 = df.set_index("company_id", drop=True)
    df2 = df2[["naam", "url_id", "levenshtein", "string_match", "ranking", "url_rank", "url_rank2"]]
    df2.dropna(how="any", axis=0, inplace=True)

    logger.debug("don")

    # df.plot.scatter(x="levenshtein", y="url_rank", c="blue")
    # df.plot.scatter(x="levenshtein", y="url_rank2", c="red")
    # df.plot.scatter(x="url_rank", y="url_rank2", c="green")
    df.plot(y=["url_rank", "url_rank2"], style=".")

    plt.show()


if __name__ == "__main__":
    # plot_website()
    plot_processing_time()
