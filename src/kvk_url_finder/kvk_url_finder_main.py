"""
Utility to import kvk/url combinations and turn it into a mysql data base

Usage:
    python kvk_url_finder_main.py URL_kvk.csv.bz2  --max 10000

With --max you can limit the number of lines read from the csv file. In case the script is called
multiple times, you continue on last kvk you have stored in the sql database

The script can be runned with kernprof in order to time all the lines

kernprof -l kvk_url_finder_main.py URL_kvk.csv.bz2  --max 10000


This generates a file kvk_url_finder_main.py.prof

Alternatively you can use the profiling tool:


profiling --dump=kvk.prof kvk_url_finder_main.py -- URL_kvk.csv.bs2 --max 100 --extend

Note that the first '--' indicates that the rest of the arguments belong to the python script and
not to profiling

"""

import argparse
import datetime
import dateutil
import getpass
import logging
import logging.config
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
import pytz

import pandas as pd
import pytimeparse
import yaml

from cbs_utils import Q_
from cbs_utils.misc import (Chdir, make_directory)
from kvk_url_finder import LOGGER_BASE_NAME, CACHE_DIRECTORY
from kvk_url_finder.kvk_engine import KvKUrlParser
from kvk_url_finder.models import DATABASE_TYPES
from kvk_url_finder.utils import (setup_logging, merge_external_database, read_database_selection)

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
    parser.add_argument("--log_file_base", default="log_kvk", help="Default logging name")
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
    parser.add_argument("--kvk_start", type=int,
                        help="Start processing at this kvk number. This overrules the setting in"
                             "the yaml file if given")
    parser.add_argument("--kvk_stop", type=int,
                        help="Stop processing at this kvk number. This overrules the setting in"
                             "the yaml file if given")
    parser.add_argument("--n_processes", type=check_positive, help="Number of processes to run",
                        default=1)
    parser.add_argument("--max_entries", type=check_positive,
                        help="Maximum number of kvk entries to process")
    parser.add_argument("--process_nr", type=check_not_negative,
                        help="Impose the default process number", default=0)
    parser.add_argument("--database_type", default=None, choices=DATABASE_TYPES,
                        help="Type of database to use. If not given, select from the settings file "
                             "or take postgres")
    parser.add_argument("--user", action="store",
                        help="Username of the postgres database. By default use current user")
    parser.add_argument("--password", action="store",
                        help="Password of the postgres database")
    parser.add_argument("--older_time", action="store",
                        help="Only process queries if older than this")
    parser.add_argument("--hostname", action="store",
                        help="Name of the host. Leave empty on th cluster. "
                             "Or set localhost at your own machine")
    parser.add_argument("--dumpdb", action="store",
                        help="Filename to dump the database to")
    parser.add_argument("--export_dataframes", action="store",
                        help="Filename of the base of all the dataframes to dump to")
    parser.add_argument("--rescan_missing_urls", action="store_true",
                        help="Set true in order to process only the kvk entries with missing urls")
    parser.add_argument("--apply_selection", action="store_true",
                        help="Force to apply the selection")
    parser.add_argument("--kvk_list", help="Give a comman separated list of kvk numbers to process"
                                           " such as  1,2,3 or just on number")
    parser.add_argument("--no_internet", action="store_true",
                        help="Run the whole process with connecting to intennet")
    parser.add_argument("--force_ssl_check", action="store_true",
                        help="Force to check the ssl https schame. If false (default) try"
                             "to get it from previous run")
    parser.add_argument("--timezone", default="Europe/Amsterdam", help="Specify the time zone")

    # parse the command line
    parsed_arguments = parser.parse_args(args)

    return parsed_arguments, parser


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
    force_ssl_check = general.get("force_ssl_check", False)
    if args.force_ssl_check:
        force_ssl_check = True
    if args.no_internet:
        internet_scraping = False
    search_urls = general.get("search_urls", False)
    max_cache_dir_size_str = general.get("max_cache_dir_size", None)
    certificate = general.get("certificate")
    if certificate is not None:
        os.environ["REQUEST_CA_BUNDLE"] = certificate
    if args.older_time is None:
        older_time_str = general.get("older_time", None)
    else:
        older_time_str = args.older_time
    if older_time_str:
        # use pytimeparse to allow general string notition of delta time, 1 h, 3 days, etc
        older_time_in_secs = pytimeparse.parse(older_time_str)
        if older_time_in_secs is None:
            # parse gave none, perhaps we have a normal time. Convert to calculate the time diff
            timezone = pytz.timezone(args.timezone)
            current_time = datetime.datetime.now(timezone)
            other_time = dateutil.parser.parse(older_time_str).astimezone(timezone)
            delta_time = current_time - other_time
            older_time_in_secs = delta_time / pd.to_timedelta(1, 's')
        # convert delta time to differnce in seconds
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

    selection_settings = settings.get("kvk_selection")
    kvk_selection_file_name = None
    kvk_selection_kvk_nummer = None
    kvk_selection_kvk_sub_nummer = None

    process_settings = settings["process_settings"]
    n_url_count_threshold = process_settings["n_url_count_threshold"]
    kvk_range_read = process_settings["kvk_range_read"]
    kvk_range_process = process_settings["kvk_range_process"]
    kvk_range_read["selection"] = None
    kvk_range_process["selection"] = None
    maximum_entries = process_settings["maximum_entries"]
    impose_url_for_kvk = process_settings["impose_url_for_kvk"]
    threshold_distance = process_settings["threshold_distance"]
    threshold_string_match = process_settings["threshold_string_match"]

    # store a list of url filter if given in the process file
    kvk_filters = process_settings.get('kvk_filters')
    if kvk_filters and kvk_filters["apply_filters"]:
        filter_kvks = kvk_filters["filters"]
    else:
        filter_kvks = list()
    url_filters = process_settings.get('url_filters')
    if url_filters and url_filters["apply_filters"]:
        filter_urls = url_filters["filters"]
    else:
        filter_urls = list()

    if args.kvk_start is not None:
        kvk_range_process["start"] = args.kvk_start
    if args.kvk_stop is not None:
        kvk_range_process["stop"] = args.kvk_stop
    if args.max_entries is not None:
        maximum_entries = args.max_entries

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

        # check if we want to take a selection by looking in the selection section of the yaml
        # file or by looking at the command line arguments
        if (selection_settings and selection_settings["apply_selection"]) or args.apply_selection:
            # get the selection type: either list (if we give a list) or database (in case we
            # get the numbers from an excel file
            if selection_settings and args.kvk_list is None:
                selection_type = selection_settings.get("type", 'kvk_list')
            else:
                selection_type = 'kvk_list'
            assert selection_type in ("database", "kvk_list")

            if selection_type == "database":
                selection_db_name = selection_settings["database"]
                selection_db = databases[selection_db_name]
                kvk_selection_file_name = selection_db["file_name"]
                kvk_selection_kvk_nummer = selection_db["kvk_nummer"]
                kvk_selection_kvk_sub_nummer = selection_db["kvk_sub_nummer"]
                kvk_selection = read_database_selection(kvk_selection_file_name,
                                                        kvk_selection_kvk_nummer)
            else:
                if args.kvk_list:
                    kvk_selection_str = args.kvk_list
                    try:
                        kvk_selection = [int(_) for _ in kvk_selection_str.split(",")]
                    except ValueError:
                        kvk_selection = int(kvk_selection_str)
                else:
                    kvk_selection = selection_settings["kvk_list"]

            # make sure we have a list
            if not isinstance(kvk_selection, list):
                kvk_selection = [kvk_selection]
        else:
            kvk_selection = None

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

        # get the list of kvk number from the database. In case a data base is empty, it is
        # created from the input files
        kvk_parser = KvKUrlParser(
            database_name=database_name,
            database_type=database_type,
            store_html_to_cache=store_html_to_cache,
            force_process=args.force_process,
            kvk_range_process=kvk_range_process,
            n_url_count_threshold=n_url_count_threshold,
            number_of_processes=args.n_processes,
            progressbar=args.progressbar,
            address_input_file_name=address_input_file_name,
            url_input_file_name=kvk_url_file_name,
            kvk_selection=kvk_selection,
            address_keys=address_keys,
            kvk_url_keys=kvk_url_keys,
            reset_database=args.reset_database,
            extend_database=args.extend_database,
            kvk_range_read=kvk_range_read,
            maximum_entries=maximum_entries,
            log_file_base=args.log_file_base,
            log_level_file=args.log_level_file,
            hostname=args.hostname,
            password=args.password,
            user=user,
            older_time=older_time,
            filter_urls=filter_urls,
            filter_kvks=filter_kvks,
            rescan_missing_urls=args.rescan_missing_urls
        )

        if args.dumpdb:
            logger.info("Dumping database to {}".format(args.dumpdb))
            kvk_parser.export_db(args.dumpdb)
            sys.exit(0)

        # in case the database did not exist yet at the start or in case the --update option is
        # given, update the sql data base from the input files
        if args.update_sql_tables:
            kvk_parser.generate_sql_tables()

        if args.export_dataframes:
            # in case we do export, we have to import all data frames
            only_comp_df = False
        else:
            # in case we are not export but processing, in the first round only load the company
            # dataframe
            only_comp_df = True

        kvk_parser.populate_dataframes(only_the_company_df=only_comp_df, only_found_urls=True)

        if args.export_dataframes:
            logger.info("Exporting dataframes to {}".format(args.dumpdb))
            kvk_parser.export_df(args.export_dataframes)
            sys.exit(0)

        kvk_parser.get_kvk_list_per_process()
        logger.debug("Found list\n{}".format(kvk_parser.kvk_ranges))
        if not kvk_parser.database.is_closed() and args.n_processes > 1:
            # only close for multiple cores
            kvk_parser.database.close()

        # either merge the database with an external database (if the merge option is given) or
        # process all the urls
        if args.merge_database:
            if kvk_selection_kvk_sub_nummer is None:
                raise ValueError("You want to merge but did not defined a database selection with"
                                 "proper kvk selection sub numme")
            logger.info("Merge kvk selection database  {}".format(kvk_selection_file_name))
            merge_external_database(kvk_parser.CompanyTbl, kvk_selection_file_name,
                                    kvk_selection_kvk_nummer, kvk_selection_kvk_sub_nummer)
            logger.info("Goodbye for now:-)")
            sys.exit(0)

        # create the object and do you thing
        jobs = list()
        for i_proc, kvk_range in enumerate(kvk_parser.kvk_ranges):

            if use_subprocess:
                logger.info("Do not make object again for multiprocessing on windows")
                # for multiprocessing on windows, we create a command line call to the
                # utility with the proper ranges
                cmd = list()
                cmd.append(script_name)
                cmd.append(str(Path(sys.argv[1]).absolute()))
                cmd.extend(["--kvk_start", str(kvk_range["start"])])
                cmd.extend(["--kvk_stop", str(kvk_range["stop"])])
                cmd.extend(sys.argv[2:])
                cmd.extend(["--n_processes", "1"])
                cmd.extend(["--nosubprocess"])
                cmd.extend(["--process_nr", str(i_proc)])
                cmd.extend(["--write_log"])
                cmd.extend(["--log_file_base", "{}_{:02d}".format(args.log_file_base,
                                                                  i_proc)])
                logger.debug(cmd)
                process = subprocess.Popen(cmd, shell=False)
                jobs.append(process)
            else:
                # for linux -or- for single processing on windows, create a new object which
                # we are going to launch
                kvk_sub_parser = KvKUrlParser(
                    database_name=database_name,
                    database_type=database_type,
                    max_cache_dir_size=max_cache_dir_size,
                    search_urls=search_urls,
                    internet_scraping=internet_scraping,
                    force_ssl_check=force_ssl_check,
                    store_html_to_cache=store_html_to_cache,
                    progressbar=args.progressbar,
                    kvk_range_process=kvk_range,
                    maximum_entries=maximum_entries,
                    force_process=args.force_process,
                    impose_url_for_kvk=impose_url_for_kvk,
                    threshold_distance=threshold_distance,
                    threshold_string_match=threshold_string_match,
                    i_proc=i_proc + args.process_nr,
                    number_of_processes=args.n_processes,
                    log_file_base=args.log_file_base,
                    log_level_file=args.log_level_file,
                    singlebar=args.singlebar,
                    password=args.password,
                    user=user,
                    hostname=args.hostname,
                    older_time=older_time,
                    filter_urls=filter_urls,
                    filter_kvks=filter_kvks,
                    rescan_missing_urls=args.rescan_missing_urls
                )
                # populate the dataframes again, now including all tables
                kvk_sub_parser.populate_dataframes()

                if args.n_processes > 1:
                    # we should not be running on windows if we are here
                    assert platform.system() != "Windows"
                    kvk_sub_parser.start()
                else:
                    # for one cpu we can directly call run
                    kvk_sub_parser.run()

                jobs.append(kvk_sub_parser)

        if args.n_processes > 1:
            if not use_subprocess:
                # this will block the script until all jobs are done
                for job in jobs:
                    job.join()

                for i_proc, process in enumerate(jobs):
                    db = process.database
                    if not db.is_closed():
                        logger.info(f"Closing process {i_proc} ")
                        db.close()
            else:
                for ip, process in enumerate(jobs):
                    logger.info("Waiting for process {} : {}".format(ip, process.pid))
                    try:
                        os.waitpid(process.pid, 0)
                        logger.debug("DONE: {} : {}".format(ip, process.pid))
                    except ChildProcessError:
                        logger.debug("NoMore: {} : {}".format(ip, process.pid))

        logger.info("Goodbye!")
        logger.debug("Really:-)")


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
