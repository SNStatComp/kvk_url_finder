import datetime
import logging
import re

from cbs_utils.misc import (create_logger, merge_loggers)
from kvk_url_finder import LOGGER_BASE_NAME

logger = logging.getLogger(LOGGER_BASE_NAME)


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


def paste_strings(string_list: list, separator=",", max_length=256, max_cnt=1000):
    """ join the string from the list upto a maximum length """

    # note that we reverse the list, we can can peel off from the back
    result = separator.join(string_list)
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
                logger.warning("Max count reached. Something wrong ?")
                break

        # reverse back
        result = result[-1:0:-1]

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
    merge_loggers(_logger, "cbs_utils", logger_level_to_merge=log_level_file)

    return _logger
