"""
Utility to import kvk/url combinations and turn it into a mysql data base

Usage:
    python find_kvk_urls.py URL_kvk.csv.bz2  --max 10000

With --max you can limit the number of lines read from the csv file. In case the script is called
multiple times, you continue on last kvk you have stored in the sql database

The script can be runned with kernprof in order to time all the lines

kernprof -l find_kvk_urls.py URL_kvk.csv.bz2  --max 10000


This generates a file find_kvk_urls.py.prof

Alternatively you can use the profiling tool:


profiling --dump=kvk.prof find_kvk_urls.py -- URL_kvk.csv.bs2 --max 100 --extend

Note that the first '--' indicates that the rest of the arguments belong to the python script and
not to profiling


have
"""

import argparse
import logging
import os
import platform
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from cbs_utils.misc import (create_logger, merge_loggers, Chdir, make_directory)
from kvk_url_finder.engine import KvKUrlParser
from kvk_url_finder.models import connect_database

try:
    from kvk_url_finder import __version__
except ModuleNotFoundError:
    __version__ = "unknown"

# set up global logger
logger: logging.Logger = None


def _parse_the_command_line_arguments(args):
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
    parser.add_argument("--write_log_to_file", action="store_true",
                        help="Write the logging information to file")
    parser.add_argument("--log_file_base", default="log", help="Default name of the logging output")
    parser.add_argument('--log_file_verbose', help="Be verbose to file", action="store_const",
                        dest="log_level_file", const=logging.INFO, default=logging.INFO)
    parser.add_argument('--log_file_quiet', help="Be quiet: no output to file",
                        action="store_const", dest="log_level_file", const=logging.WARNING)
    parser.add_argument("--update_sql_tables", action="store_true",
                        help="Reread the csv file with urls/addresses and update the tables ")
    parser.add_argument("--force_process", action="store_true",
                        help="Force to process company table, even if they have been marked "
                             "as processes")
    parser.add_argument("--merge_database", action="store_true",
                        help="Merge the current sql data base marked to the selection data base")
    parser.add_argument("--n_processes", type=int, help="Number of processes to run", default=1,
                        choices=range(1, 8))

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

    if write_log_to_file:
        # http://stackoverflow.com/questions/29087297/
        # is-there-a-way-to-change-the-filemode-for-a-logger-object-that-is-not-configured
        # sys.stderr = open(log_file_base + ".err", 'w')
        pass
    else:
        log_file_base = None

    _logger = create_logger(name=__name__,
                            file_log_level=log_level_file,
                            console_log_level=log_level,
                            log_file=log_file_base)

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
    merge_loggers(_logger, "cbs_utils")
    merge_loggers(_logger, "kvk_url_finder.engine")

    _logger.info("{:10s}: {}".format("Running", sys.argv))
    _logger.info("{:10s}: {}".format("Version", __version__))
    _logger.info("{:10s}: {}".format("Directory", os.getcwd()))
    _logger.debug("Debug message")

    return _logger


def main(args_in):
    args, parser = _parse_the_command_line_arguments(args_in)

    # with the global statement line we make sure to change the global variable at the top
    # when settin gup the logger
    with open(args.configuration_file, "r") as stream:
        settings = yaml.load(stream=stream)

    general = settings["general"]
    working_directory = general["working_directory"][platform.system()]
    cache_directory = general["cache_directory"]
    output_directory = general["output_directory"]
    database_name = general.get("database_name", "kvk_db.sqlite")

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
    maximum_entries = process_settings["maximum_entries"]
    impose_url_for_kvk = process_settings["impose_url_for_kvk"]
    threshold_distance = process_settings["threshold_distance"]
    threshold_string_match = process_settings["threshold_string_match"]

    # create the KvKUrl object, but first move to the workding directory, so everything we do
    # is with respect to this directory
    with Chdir(working_directory) as _:

        global logger
        logger = setup_logging(
            write_log_to_file=args.write_log_to_file,
            log_file_base=args.log_file_base,
            log_level_file=args.log_level_file,
            log_level=args.log_level,
            progress_bar=args.progressbar
        )

        # with the global statement line we make sure to change the global variable at the top

        script_name = os.path.basename(sys.argv[0])
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
        make_directory(cache_directory)
        make_directory(output_directory)

        # connect to the sqlite database
        db_file_name = Path(output_directory) / database_name
        db_exists_before_connecting = db_file_name.exists()
        logger.info("Connecting to database {}".format(db_file_name))
        connect_database(db_file_name, reset_database=args.reset_database)

        # get the list of kvk number from the database. In case a data base is empty, it is
        # created from the input files
        kvk_parser = KvKUrlParser(
            cache_directory=cache_directory,
            force_process=args.force_process,
            kvk_range_process=kvk_range_process,
            n_url_count_threshold=n_url_count_threshold,
            number_of_processes=args.n_processes,
            progressbar=args.progressbar,
            address_input_file_name=address_input_file_name,
            url_input_file_name=kvk_url_file_name,
            kvk_selection_input_file_name=kvk_selection_file_name,
            kvk_selection_kvk_key=kvk_selection_kvk_nummer,
            kvk_selection_kvk_sub_key=kvk_selection_kvk_sub_nummer,
            address_keys=address_keys,
            kvk_url_keys=kvk_url_keys,
            reset_database=args.reset_database,
            extend_database=args.extend_database,
            kvk_range_read=kvk_range_read,
            maximum_entries=maximum_entries,
            log_file_base=args.log_file_base,
            log_level_file=args.log_level_file,
        )
        # in case the database did not exist yet at the start or in case the --update option is
        # given, update the sql data base from the input files
        if args.update_sql_tables or not db_exists_before_connecting:
            kvk_parser.generate_sql_tables()
        kvk_parser.get_kvk_list_per_process()
        logger.debug("Found list\n{}".format(kvk_parser.kvk_ranges))

        # either merge the database with an external database (if the merge option is given) or
        # process all the urls
        if args.merge_database:
            kvk_parser.merge_external_database()
        else:

            # create the object and do you thing
            for i_proc, kvk_range in enumerate(kvk_parser.kvk_ranges):
                kvk_parser = KvKUrlParser(
                    cache_directory=cache_directory,
                    progressbar=args.progressbar,
                    kvk_range_process=kvk_range,
                    maximum_entries=maximum_entries,
                    force_process=args.force_process,
                    impose_url_for_kvk=impose_url_for_kvk,
                    threshold_distance=threshold_distance,
                    threshold_string_match=threshold_string_match,
                    i_proc=i_proc,
                    number_of_processes=args.n_processes,
                    log_file_base=args.log_file_base,
                    log_level_file=args.log_level_file,
                    singlebar=args.singlebar,
                )

                if args.n_processes > 1:
                    # start is the multiprocessing.Process method that calls the run method of
                    # our class.
                    kvk_parser.start()
                else:
                    # for one cpu we can directly call run
                    kvk_parser.run()


def _run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == '__main__':
    _run()
