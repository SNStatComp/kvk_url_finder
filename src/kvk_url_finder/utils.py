import datetime
import logging
import re
import Levenshtein
import tldextract
import difflib
import pandas as pd

from cbs_utils.misc import (create_logger, merge_loggers, standard_postcode)
from cbs_utils.web_scraping import UrlSearchStrings
from kvk_url_finder import LOGGER_BASE_NAME
from kvk_url_finder.models import (POSTAL_CODE_KEY, KVK_KEY, BTW_KEY, NAME_KEY)

logger = logging.getLogger(LOGGER_BASE_NAME)


class UrlCompanyRanking(object):
    """
    Class do perform all operation to match a url
    """

    def __init__(self, url, company_name, url_extract=None, url_analyse=None,
                 company_postcodes=None, company_kvk_nummer=None, company_btw_nummer=None,
                 threshold_string_match=None, threshold_distance=None, logger=None,
                 max_url_score=3):

        self.logger = logger
        self.company_name = company_name
        self.url = url

        self.matched_postcode = None
        self.matched_kvk_nummer = None
        self.company_postcodes = company_postcodes
        self.company_kvk_nummer = company_kvk_nummer
        self.company_btw_nummer = company_btw_nummer

        self.url_analyse = url_analyse
        self.threshold_string_match = threshold_string_match
        self.threshold_distance = threshold_distance
        self.max_url_score = max_url_score

        if url_extract is None:
            self.ext = tldextract.extract(url)
        else:
            # we have passed the tld extract as an argument
            self.ext = url_extract

        self.ranking = 0

        self.distance: int = None
        self.string_match: float = None
        self.url_match: float = None
        self.url_rank: float = None

        self.has_postcode = False
        self.has_kvk_nummer = False
        self.has_btw_nummer = False

        self.nl_company = None

        self.best_match = False

        self.kvk_nummer = None
        self.btw_nummer = None

        self.postcode_set = set()
        self.kvk_set = set()
        self.btw_set = set()

        self.get_levenstein_distance()
        self.get_string_match()

        self.rank_contact_list()
        self.get_ranking()

    def get_levenstein_distance(self):
        """
        Get the levenstein distance of the company name
        """

        # the subdomain may also contain the relevant part, e.g. for ramlehapotheek.leef.nl,
        # the sub domain is ramlehapotheek, which is closer to the company name the the
        # domain leef. Therefore pick the minimum
        subdomain_dist = Levenshtein.distance(self.ext.subdomain, self.company_name)
        domain_dist = Levenshtein.distance(self.ext.domain, self.company_name)
        self.distance = min(subdomain_dist, domain_dist)

    def get_string_match(self):
        """
        Get the string match. Th match is given by a float value between 0 (no match and 1 (fully
        matched)
        """
        subdomain_match = difflib.SequenceMatcher(None, self.ext.subdomain,
                                                  self.company_name).ratio()
        domain_match = difflib.SequenceMatcher(None, self.ext.domain,
                                               self.company_name).ratio()
        self.string_match = max(subdomain_match, domain_match)

    def rank_contact_list(self):
        """
        Give extra score to the btw in case btw number, postcode and kvk number occur at the
        same page, as it is more likely that this page contains the contact info of the company

        """

        # create dataframe with the postcode, kvk and btw. for each occurrence of one of the items,
        # add one
        df = pd.DataFrame(index=[POSTAL_CODE_KEY, KVK_KEY, BTW_KEY])
        for key, url_p_m in self.url_analyse.url_per_match.items():
            for match, url in url_p_m.items():
                if url not in df.columns:
                    df[url] = 0
                df.loc[key, url] += 1

        # clip the count per item to 0 or 1 (no or at least one occurance)
        contact_hits_per_url = df.astype(bool).sum()

        # create a data frame for all urls per match in which we have the match and number of url
        for key, url_p_m in self.url_analyse.url_per_match.items():
            match_list = list()
            url_score = list()
            for match, url in url_p_m.items():
                match_list.append(match)
                url_score.append(contact_hits_per_url[url])
            match_df = pd.DataFrame(zip(match_list, url_score), columns=["match", "score"])
            match_df.sort_values(["score"])

            # overwrite the match list of the current column postcode, kvk, btw such that the
            # values with many other items is on top
            self.url_analyse.matches[key] = list(match_df["match"].values)

        self.logger.debug("got sorted url {}".format(self.url_analyse))

    def get_ranking(self):

        if self.url_analyse:
            postcode_lijst = self.url_analyse.matches[POSTAL_CODE_KEY]
            kvk_lijst = self.url_analyse.matches[KVK_KEY]
            btw_lijst = self.url_analyse.matches[BTW_KEY]
        else:
            self.logger.debug("Skipping Url Search : {}".format(self.url))
            # if we did not scrape the internet, set the postcode_lijst eepty
            postcode_lijst = list()
            kvk_lijst = list()
            btw_lijst = list()

        # turn the lists into set such tht we only get the unique values
        self.postcode_set = set([standard_postcode(pc) for pc in postcode_lijst])
        self.kvk_set = set([int(re.sub(r"\.", "", kvk)) for kvk in kvk_lijst])
        self.btw_set = set([re.sub(r"\.", "", btw) for btw in btw_lijst])

        if self.company_postcodes:
            post_codes_on_side = self.company_postcodes.intersection(self.postcode_set)
            if post_codes_on_side:
                self.has_postcode = True
                self.matched_postcode = list(post_codes_on_side)[0]
                self.ranking += 3
                self.logger.debug(f"Found matching postcode. Added to ranking {self.ranking}")
        else:
            self.has_postcode = False

        if self.company_kvk_nummer in self.kvk_set:
            self.has_kvk_nummer = True
            self.matched_kvk_nummer = self.company_kvk_nummer
            self.ranking += 3
            self.logger.debug(f"Found matching kvknummer code {self.company_kvk_nummer}. "
                              f"Added to ranking {self.ranking}")

        if self.btw_set:
            self.has_btw_nummer = True
            self.btw_nummer = re.sub(r"\.", "", list(self.btw_set)[0])
            self.ranking += 0  # for now we dont give a score to btw because we cannot validate it
            self.logger.debug(f"Found matching btw number {self.btw_nummer}. "
                              f"Added to ranking {self.ranking}")
        else:
            self.btw_nummer = None

        # calculate the url match based on the levenshtein distance and string match
        self.url_match = self.distance * (1 - self.string_match)
        rel_score = max((1 - self.url_match / self.threshold_distance), 0)
        self.url_rank = self.max_url_score * rel_score ** 2  # quick drop off for lower scores

        if self.ext.suffix in ("com", "org", "eu"):
            self.url_rank += 0.1
        elif self.ext.suffix == "nl":
            self.url_rank += 0.2

        if self.ext.subdomain == "www":
            self.url_rank += 0.1
        elif self.ext.subdomain == "":
            self.url_rank += 0.1

        # add the url matching score
        self.ranking += self.url_rank

        if self.has_btw_nummer or self.has_kvk_nummer or self.has_postcode:
            # if the side contains any company info, set the nl_company_flag to true
            self.nl_company = True
        else:
            self.nl_company = False


class UrlInfo(object):
    """
    Class to hold all the properties of one single web site
    """

    def __init__(self, index, url):
        self.index = index
        self.needs_update = False
        self.url = url
        self.url_extract = tldextract.extract(url)
        self.outside_nl = False
        self.processing_time: datetime.datetime = None
        self.url_analyse: UrlSearchStrings = None
        self.match: UrlCompanyRanking = None
        self.btw_numbers = None
        self.kvk_numbers = None
        self.psc_numbers = None
        self.ecommerce = None
        self.social_media = None
        self.category = None
        self.referred_by = None


class Range(object):
    """
    A class holding the range of kvk numbers

    Parameters
    ----------
    range_dict: dict
        dictionary with two fields:
            * start: int or str
                Start kvk number or url to process
            * stop: int or str
                End kvk number or url to process
    """

    def __init__(self, range_dict):
        if range_dict is not None:
            self.start = range_dict["start"]
            self.stop = range_dict["stop"]
        else:
            self.start = None
            self.stop = None


def paste_strings(string_list: list, separator=",", max_length=256, max_cnt=10000000):
    """ join the string from the list upto a maximum length """

    # note that we reverse the list, we can can peel off from the back
    try:
        result = separator.join(string_list)
    except TypeError:
        result = None
    else:
        # matches the fist item (? is non-greedy)
        length = len(result)
        if length > max_length:
            result = result[-1:0:-1]  # reverse string to be able to peel off from the back
            match = re.compile(r"^.*?" + separator)
            cnt = 0
            while length > max_length:
                result = match.sub("", result)
                length = len(result)
                cnt += 1
                if cnt > max_cnt:
                    try:
                        result = result[:max_length]
                    except IndexError:
                        pass
                    logger.warning("Max count reached. Something wrong ?")
                    break

            # reverse back
            result = result[-1:0:-1]

    if result == "":
        result = None

    return result


def check_if_url_needs_update(processing_time: datetime.datetime,
                              current_time: datetime.datetime,
                              older_time: datetime.timedelta,
                              ):
    """
    Check the url to see if it needs updated or not based on the last processing time

    Parameters
    ----------
    processing_time: datetime.datetime
        Time of last processing of the url
    current_time: datetime.datetime
        Current time of processing
    older_time: datetime.timedelta
        Update the url in case it was processed longer than 'older_time' ago

    Returns
    -------
    bool:
        True in case it needs update
    """

    url_needs_update = True
    logger.debug("processing time {} ".format(processing_time))
    if processing_time and older_time:
        delta_time = current_time - processing_time
        logger.debug(f"Processed with delta time {delta_time}")
        if delta_time < older_time:
            logger.debug(f"Less than {older_time}. Skipping")
            url_needs_update = False
        else:
            logger.debug(
                f"File was processed more than {older_time} ago. Do it again!")
    else:
        # we are not skipping this file and we have a url_nl reference. Store the
        # current processing time
        logger.debug(f"We are updating the url_nl datetime {current_time}")

    return url_needs_update


def setup_logging(logger_name=None,
                  write_log_to_file=False,
                  log_file_base="log",
                  log_level_file=logging.INFO,
                  log_level=None,
                  progress_bar=False,
                  ):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Initialise the logging system
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    if write_log_to_file or progress_bar:
        # http://stackoverflow.com/questions/29087297/
        # is-there-a-way-to-change-the-filemode-for-a-logger-object-that-is-not-configured
        # sys.stderr = open(log_file_base + ".err", 'w')
        pass
    else:
        log_file_base = None

    if logger_name is None:
        name = LOGGER_BASE_NAME
    else:
        name = logger_name

    formatter_long = logging.Formatter('[%(asctime)s] %(name)-5s %(levelname)-8s --- %(message)s ' +
                                       '(%(filename)s:%(lineno)s)', datefmt='%Y-%m-%d %H:%M:%S')
    _logger = create_logger(name=name,
                            file_log_level=log_level_file,
                            console_log_level=log_level,
                            log_file=log_file_base,
                            formatter_file=formatter_long,
                            console_log_format_long=True,
                            )

    if progress_bar:
        # switch off all logging because we are showing the progress bar via the print statement
        # logger.disabled = True
        # logger.disabled = True
        # logger.setLevel(logging.CRITICAL)
        for handle in _logger.handlers:
            try:
                getattr(handle, "baseFilename")
            except AttributeError:
                # this is the stream handle because we get an AtrributeError. Set it to critical
                handle.setLevel(logging.CRITICAL)

    # with this call we merge the settings of our logger with the logger in the cbs_utils logger
    # so we can control the output
    cbs_utils_logger = logging.getLogger("cbs_utils")
    cbs_utils_logger.setLevel(log_level)

    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    # _logger.addHandler(handler)
    # cbs_utils_logger.addHandler(handler)
    merge_loggers(_logger, "cbs_utils", logger_level_to_merge=log_level)

    return _logger


def read_sql_table(table_name, connection, sql_command=None,
                   variable=None,
                   datetime_key=None,
                   lower=None,
                   upper=None,
                   max_query=None,
                   reset=True,
                   force_process=False,
                   older_time=None,
                   selection=None):

    cache_file = table_name + ".pkl"
    if sql_command is None:
        sql_command = f"select * from {table_name}"

    if variable is not None:
        # column  name is given. See if we need to filter on a range of this column
        if lower is not None or upper is not None:
            if lower is not None and upper is not None:
                sql_command += " " + f"where {variable} between {lower} and {upper}"
            elif lower is None and upper is not None:
                sql_command += " " + f"where {variable} <= {upper}"
            elif lower is not None and upper is None:
                sql_command += " " + f"where {variable} >= {lower}"
        elif selection is not None:
            # if no lower and upper range is given, perhaps we have an explicit list of values
            sql_command += " " + "where {} in ({})".format(variable,
                                                           ",".join([str(_) for _ in selection]))

    if not force_process and datetime_key is not None:
        if older_time is not None:
            # only select the rows processed longer the 'older_time' ago
            start_time = datetime.datetime.now() - older_time
            sql_command += f" and ({datetime_key} < '{start_time}' or {datetime_key} is null)"
        else:
            # only select the non processed rows
            sql_command += f" and {datetime_key} is null"

    if max_query is not None:
        sql_command += f" limit {max_query}"

    df = None
    if not reset:
        try:
            df = pd.read_pickle(cache_file)
            logger.info(f"Read table pickle file {cache_file}")
        except IOError:
            logger.debug(f"No pickle file available {cache_file}")

    if df is None:
        logger.info("Connecting to database")
        logger.info(f"Start reading table from postgres table {table_name}")
        df = pd.read_sql(sql_command, con=connection)
        logger.info(f"Dumping to pickle file {cache_file}")
        df.to_pickle(cache_file)
        logger.info("Done")

    return df


def get_string_name_from_df(column_name, row, index, dataframe):
    """
    In case a name in a row has the form of a datetime, it is automatically converted to a
    datestring, which is not what we want.

    Parameters
    ----------
    column_name: str
        Name of the column to extract
    row: Series
        Row from the dataframe from iterows
    dataframe: DataFrame
        Full DataFrma from the dataframe from iterows

    Returns
    -------
    str:
        Name obtained from the column

    """
    col_str = row[column_name]
    if not isinstance(col_str, str):
        # in case a name has the form of a date/time, it is automatically converted by
        # iterows. Convert to original
        col_str = dataframe.loc[index, column_name]

    return col_str
