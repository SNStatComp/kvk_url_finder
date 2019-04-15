"""
Utility to import a url database with unique urls and check if they are belong to a dutch company

Usage:
    python url_to_kvk.py URL_kvk.csv.bz2  --max 10000

"""

import argparse
import getpass
import logging
import logging.config
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
import datetime
import collections
import pytimeparse

import pandas as pd
import yaml

from cbs_utils.misc import (create_logger, Chdir, make_directory, merge_loggers)
from cbs_utils import Q_
from kvk_url_finder import LOGGER_BASE_NAME, CACHE_DIRECTORY
from kvk_url_finder.url_engine import UrlParser
from kvk_url_finder.models import DATABASE_TYPES

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"


def _parse_the_command_line_arguments(args):
    def check_positive(value):
        """ local function to test if an argument is larger than zero"""
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError("{} is an invalid positive int value".format(value))
        return ivalue

    def check_not_negative(value):
        """ local function to test if an argument is larger than zero"""
        ivalue = int(value)
        if ivalue < 0:
            raise argparse.ArgumentTypeError("{} is an invalid negative int value".format(value))
        return ivalue

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # parse the command line to set some options2
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser(description='Parse a CSV file with KVK URLs',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # set the verbosity level command line arguments
    # mandatory arguments
    parser.add_argument("configuration_file", action="store",
                        help="The yaml settings file")
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
    parser.add_argument('--progressbar', help="Show a progress bar", action="store_true")
    parser.add_argument('--singlebar', help="Only show one bar for multiprocessing",
                        action="store_true")
    parser.add_argument('--reset_database', help="Reset the data base in case we have generated"
                                                 "a sql file already", action="store_true")
    parser.add_argument('--extend_database', help="Extend the data base in case we have generated"
                                                  "a sql file already", action="store_true")
    parser.add_argument("--write_log_to_file", action="store_true", default=False,
                        help="Write the logging information to file")
    parser.add_argument("--no_write_log_to_file", action="store_false", dest="write_log_to_file",
                        help="Do not write the logging information to file")
    parser.add_argument("--log_file_base", default="log", help="Default name of the logging output")
    parser.add_argument('--log_file_debug', help="Be very verbose to file", action="store_const",
                        dest="log_level_file", const=logging.DEBUG, default=logging.INFO)
    parser.add_argument('--log_file_verbose', help="Be verbose to file", action="store_const",
                        dest="log_level_file", const=logging.INFO, default=logging.INFO)
    parser.add_argument('--log_file_quiet', help="Be quiet: no output to file",
                        default=logging.INFO, action="store_const", dest="log_level_file",
                        const=logging.WARNING)
    parser.add_argument("--update_sql_tables", action="store_true",
                        help="Reread the csv file with urls/addresses and update the tables ")
    parser.add_argument("--force_process", action="store_true",
                        help="Force to process company table, even if they have been marked "
                             "as processes")
    parser.add_argument("--subprocess", action="store_true",
                        help="Force to use subprocess, even on Linux ")
    parser.add_argument("--nosubprocess", action="store_false", dest="subprocess",
                        help="Prevent to use the forced subprocess ")
    parser.add_argument("--merge_database", action="store_true",
                        help="Merge the current sql data base marked to the selection data base")
    parser.add_argument("--n_processes", type=check_positive, help="Number of processes to run",
                        default=1)
    parser.add_argument("--process_nr", type=check_not_negative,
                        help="Impose the default process number", default=0)
    parser.add_argument("--maximum_entries", type=check_not_negative,
                        help="Maximum number of urls to process", default=None)
    parser.add_argument("--database_type", default=None, choices=DATABASE_TYPES,
                        help="Type of database to use. If not given, select from the settings file "
                             "or take postgres")
    parser.add_argument("--user", action="store",
                        help="Username of the postgres database. By default use current user")
    parser.add_argument("--password", action="store",
                        help="Password of the postgres database")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")
    parser.add_argument("--dumpdb", action="store",
                        help="Filename to dump the database to")
    parser.add_argument("--start_url", action="store",
                        help="Start processing at this url from the database")
    parser.add_argument("--stop_url", action="store",
                        help="Stop processing at this url from the database")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


def setup_logging(write_log_to_file=False,
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

    formatter_long = logging.Formatter('[%(asctime)s] %(name)-5s %(levelname)-8s --- %(message)s ' +
                                       '(%(filename)s:%(lineno)s)', datefmt='%Y-%m-%d %H:%M:%S')
    _logger = create_logger(name=LOGGER_BASE_NAME,
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


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    # with the global statement line we make sure to change the global variable at the top
    # when settings gup the logger
    with open(args.configuration_file, "r") as stream:
        settings = yaml.load(stream=stream, Loader=yaml.FullLoader)

    general = settings["general"]
    working_directory = general["working_directory"][platform.system()]
    output_directory = general["output_directory"]
    database_name = general.get("database_name", "kvk_db")
    store_html_to_cache = general.get("store_html_to_cache", False)
    internet_scraping = general.get("internet_scraping", True)
    search_urls = general.get("search_urls", False)
    max_cache_dir_size_str = general.get("max_cache_dir_size", None)
    certificate = general.get("certificate")
    if certificate is not None:
        os.environ["REQUEST_CA_BUNDLE"] = certificate
    older_time_str = general.get("older_time", None)
    if older_time_str:
        # use pytimeparse to allow general string notition of delta time, 1 h, 3 days, etc
        older_time_in_secs = pytimeparse.parse(older_time_str)
        older_time = datetime.timedelta(seconds=older_time_in_secs)
    else:
        older_time = None

    # this allows us to use the Pint conversion where MB or GB can be recognised. One flaw: in
    # Pint 1GB = 1000 MB = 1000000 kB. Normally this should be 1024 and 1024 * 1024, etc
    if max_cache_dir_size_str is not None:
        max_cache_dir_size = Q_(max_cache_dir_size_str).to("B").magnitude
    else:
        max_cache_dir_size = None

    if args.database_type is not None:
        database_type = args.database_type
    else:
        database_type = general.get("database_type", "postgres")
    assert database_type in DATABASE_TYPES

    databases = settings["databases"]
    address_db = databases['addresses']
    kvk_urls_db = databases['kvk_urls']
    address_input_file_name = address_db["file_name"]
    address_keys = address_db["keys"]
    kvk_url_file_name = kvk_urls_db["file_name"]
    kvk_url_keys = kvk_urls_db["keys"]

    selection_db = databases.get("kvk_selection_data_base")
    if selection_db and selection_db.get("apply_selection", True):
        kvk_selection_file_name = selection_db["file_name"]
        kvk_selection_kvk_nummer = selection_db["kvk_nummer"]
        kvk_selection_kvk_sub_nummer = selection_db["kvk_sub_nummer"]
    else:
        kvk_selection_file_name = None
        kvk_selection_kvk_nummer = None
        kvk_selection_kvk_sub_nummer = None

    process_settings = settings["process_settings"]
    n_url_count_threshold = process_settings["n_url_count_threshold"]
    kvk_range_read = process_settings["kvk_range_read"]
    kvk_range_process = process_settings["kvk_range_process"]
    if args.maximum_entries is not None:
        maximum_entries = args.maximum_entries
    else:
        maximum_entries = process_settings["maximum_entries"]
    impose_url_for_kvk = process_settings["impose_url_for_kvk"]
    threshold_distance = process_settings["threshold_distance"]
    threshold_string_match = process_settings["threshold_string_match"]

    # store a list of url filter if given in the process file
    url_filters = process_settings.get('url_filters')
    if url_filters and url_filters["apply_filters"]:
        filter_urls = url_filters["filters"]
    else:
        filter_urls = list()

    if (args.n_processes > 1 and platform.system() == "Windows") or args.subprocess:
        use_subprocess = True
    else:
        use_subprocess = False

    # create the KvKUrl object, but first move to the working directory, so everything we do
    # is with respect to this directory
    with Chdir(working_directory) as _:

        logger = setup_logging(
            write_log_to_file=args.write_log_to_file,
            log_file_base=args.log_file_base,
            log_level_file=args.log_level_file,
            log_level=args.log_level,
            progress_bar=args.progressbar
        )
        logger.debug("Enter run with python version {}".format(sys.base_prefix))
        logger.debug("ARGV_IN: {}".format(" ".join(args_in)))
        args_str = ["{}:{}".format(at, getattr(args, at)) for at in dir(args) if
                    not None and not at.startswith("_")]
        logger.debug("ARGV: {}".format(" ".join(args_str)))

        # with the global statement line we make sure to change the global variable at the top

        # the logger base name is the same as the module name
        script_name = LOGGER_BASE_NAME
        if platform.system() == "Windows":
            script_name += ".exe"

        start_time = pd.to_datetime("now")
        message = "Start {script} (v: {version}) at {start_time}:\n{cmd}" \
                  "".format(script=script_name, version=__version__,
                            start_time=start_time, cmd=sys.argv[:])
        if not args.progressbar:
            logger.info(message)
        else:
            print(message)

        # change the log level to our requested level
        if args.progressbar:
            logger.setLevel(logging.INFO)

        # make the directories in case they do not exist yet
        make_directory(CACHE_DIRECTORY)
        make_directory(output_directory)

        if args.user is not None:
            user = args.user
        else:
            user = getpass.getuser().lower()

        # connect to the sqlite or postgres database
        if database_type == "sqlite":
            # only for sqlite the database is a real file
            database_name = Path(output_directory) / database_name
            if database_name.suffix == "":
                database_name = database_name.with_suffix(".sql")
            logger.info(f"Using sqlite database: {database_name}")
        else:
            logger.info(f"Using postgres database: {database_name}")

        logger.info("Goodbye!")

        url_parser = UrlParser(
            database_name=database_name,
            database_type=database_type,
            store_html_to_cache=store_html_to_cache,
            force_process=args.force_process,
            kvk_range_process=kvk_range_process,
            number_of_processes=args.n_processes,
            progressbar=args.progressbar,
            address_keys=address_keys,
            kvk_url_keys=kvk_url_keys,
            kvk_range_read=kvk_range_read,
            maximum_entries=maximum_entries,
            log_file_base=args.log_file_base,
            log_level_file=args.log_level_file,
            hostname=args.hostname,
            password=args.password,
            user=user,
            older_time=older_time,
            filter_urls=filter_urls,
            start_url=args.start_url,
            stop_url=args.stop_url,
        )

        if args.dumpdb:
            logger.info("Dumping database to {}".format(args.dumpdb))
            url_parser.export_db(args.dumpdb)
            sys.exit(0)

        url_parser.get_url_list_per_process()


def _run():
    """Entry point for console_scripts
    """
    logger = logging.getLogger(LOGGER_BASE_NAME)
    start = time.time()
    main(sys.argv[1:])
    duration = time.time() - start
    logger.info(f"Total processing time: {duration} seconds ")


if __name__ == '__main__':
    _run()
