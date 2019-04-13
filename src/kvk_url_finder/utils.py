import logging
import re

from kvk_url_finder import LOGGER_BASE_NAME

logger = logging.getLogger(LOGGER_BASE_NAME)


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

