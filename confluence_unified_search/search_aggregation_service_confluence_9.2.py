from flask import Flask, request, Response, render_template
from operator import itemgetter
import urllib.parse
from atlassian import Confluence
from collections import defaultdict
import time
from datetime import datetime, timedelta
import os
import logging
import configparser
from logging.handlers import RotatingFileHandler
import sys

# Add these imports for Windows service functionality
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# Set up more comprehensive logging
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, "confluence_search_service.log")

# Configure root logger
logging.basicConfig(
    level=logging.INFO,  # Changed from WARNING to INFO for more details
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5),  # 10MB file size
        logging.StreamHandler()  # Also output to console when run manually
    ]
)

# Create a logger specific to this service
logger = logging.getLogger('confluence_search_service')

# Suppress noisy libraries
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").propagate = False
logging.getLogger("requests").setLevel(logging.CRITICAL)


def load_config():
    try:
        # Log current working directory for debugging
        current_dir = os.getcwd()
        logger.info(f"Current working directory: {current_dir}")

        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        logger.info(f"Script directory: {script_dir}")

        # Use absolute path to config file
        config_path = os.path.join(script_dir, 'config.ini')
        logger.info(f"Looking for config file at: {config_path}")

        if not os.path.exists(config_path):
            logger.error(f"Config file not found at {config_path}")
            # Fallback to checking current directory
            alt_path = os.path.join(current_dir, 'config.ini')
            logger.info(f"Trying alternate path: {alt_path}")
            if os.path.exists(alt_path):
                config_path = alt_path
                logger.info(f"Found config at alternate path: {alt_path}")
            else:
                logger.error(f"Config file not found at alternate path either")
                # List all files in the directory to help diagnose
                logger.info(f"Files in script directory: {os.listdir(script_dir)}")
                logger.info(f"Files in current directory: {os.listdir(current_dir)}")

        config = configparser.ConfigParser()
        logger.info(f"Reading config from: {config_path}")
        config.read(config_path)

        # Check if required sections exist
        logger.info(f"Config sections found: {config.sections()}")
        if 'credentials' not in config:
            logger.error(f"'credentials' section not found in config file")
            logger.error(f"Available sections: {config.sections()}")
            if config.sections():
                for section in config.sections():
                    logger.info(f"Section '{section}' contains keys: {list(config[section].keys())}")
        else:
            logger.info("'credentials' section found in config file")
            if 'USERNAME' in config['credentials'] and 'PASSWORD' in config['credentials']:
                logger.info("Required credential keys found")
            else:
                logger.error(
                    f"Missing required keys in credentials section. Available keys: {list(config['credentials'].keys())}")

        return {
            'CONFLUENCE_URL': config['confluence']['base_url'],
            'USERNAME': config['credentials']['USERNAME'],
            'PASSWORD': config['credentials']['PASSWORD']
        }
    except Exception as e:
        logger.exception(f"Error loading configuration: {str(e)}")
        # Provide a fallback or raise the exception as needed
        raise


config = load_config()

confluence = Confluence(
    url=config['CONFLUENCE_URL'],
    username=config['USERNAME'],
    password=config['PASSWORD'],
    verify_ssl=False
)

message_queue = []


def contains_quotes(search_input):
    return '"' in search_input or "'" in search_input


def escape_quotes_in_cql(cql_query):
    """
    Escapes double quotes in a CQL query by prepending a backslash (\).

    Args:
        cql_query (str): The raw CQL query input with unescaped quotes.

    Returns:
        str: The CQL query with properly escaped quotes.
    """
    # Escape all double quotes by prepending a backslash
    escaped_query = cql_query.replace('"', '\\"')
    return escaped_query


def get_search_results(cql):
    # cql = 'text~"margin AND calculation" and type=page and not title~"Margin Team Meeting"'
    global message_queue
    message_queue = []

    results_set = []
    start = 0

    results = confluence.cql(cql=cql, start=0, limit=10000, expand=None, include_archived_spaces=False, excerpt=False)
    results_set += results['results']
    start += 500

    message_queue.append(f"{results['totalSize']} results found. Processing ...")

    while start < results['totalSize']:
        print(cql)
        results = confluence.cql(cql=cql, start=start, limit=10000, expand=None, include_archived_spaces=False,
                                 excerpt=False)
        results_set += results['results']
        message_queue.append(f"Loaded {len(results_set)}/{results['totalSize']}")
        start += 500

    print(f"Result set length: {len(results_set)}")

    space_count = defaultdict(int)
    space_dates = defaultdict(list)

    for result in results_set:
        try:
            # space_count[(result['resultGlobalContainer']['title'],
            #              result['resultGlobalContainer']['displayUrl'].replace('/display/', ''))] += 1
            space_dates[(result['resultGlobalContainer']['title'],
                         result['resultGlobalContainer']['displayUrl'].replace('/display/', ''))].append(
                result["lastModified"][:10])
        except KeyError as e:
            print(f"ERROR {e}")
            print(f"ERROR {result}")

    return space_dates


def convert_confluence_url(old_url):
    # Extract the space path from the old URL format
    import re

    # Find the pattern "/spaces/SPACENAME/overview" in the old URL
    space_pattern = r'/spaces/([^/]+)/overview'
    match = re.search(space_pattern, old_url)

    if not match:
        return "Could not find space pattern in the URL"

    # Get the space name
    space_name = match.group(1)

    # Replace the old format with the new format
    new_url = old_url.replace(f'/spaces/{space_name}/overview', f'{space_name}')

    return new_url


@app.route('/', methods=['GET', 'POST'])
def search_confluence():
    if request.method == 'POST':
        cql_query = request.form['cql_query']

        if cql_query == "":
            return render_template("search.html")

        # this is where we deal with the quotes in input, lets' see
        cql_query = escape_quotes_in_cql(cql_query)

        cql_query_hist = cql_query + '" and lastmodified > 2020-01-01'
        cql = 'text~"' + cql_query + '" and type=page'
        cql_hist = 'text~"' + cql_query_hist + ' and type=page'

        print(cql)

        all_history_page_dates = get_search_results(cql)

        timelines = []
        now = datetime.now()
        years = 10
        years_ago = now - timedelta(days=years * 365)

        temp_list = list(all_history_page_dates.items())
        temp_list.sort(key=lambda x: len(x[1]), reverse=True)

        sorted_dict = defaultdict(list)
        for key, value in temp_list:
            sorted_dict[key].extend(value)

        for key, values in sorted_dict.items():
            events = [(datetime.fromisoformat(date) - years_ago) / (now - years_ago) if (datetime.fromisoformat(
                date) - years_ago) / (now - years_ago) > 0 else 0 for date in values]
            count = len(events)
            desc = key[0]
            old_url = (
                f"{config['CONFLUENCE_URL']}/dosearchsite.action?cql=siteSearch%20~%20%22{urllib.parse.quote_plus(cql_query)}"
                f"%22%20AND%20space%20in%20(%22{key[1]}%22)%20AND%20type%20in%20(%22page%22)&includeArchivedSpaces=false")

            # megahack - upgrade to confluence 9.2  (from 6.17) broke the syntax
            url = convert_confluence_url(old_url)

            print(url)

            timelines.append((events, desc, url, count))

        return render_template("timeline.html", timelines=timelines, now=now, cql=cql_query)

    else:
        return render_template("search.html")


@app.route('/status')
def status():
    def generate_status():
        while True:
            if message_queue:
                message = message_queue.pop(0)
                yield 'data: %s\n\n' % message
            else:
                yield '.'

            time.sleep(1)

    return Response(generate_status(), mimetype='text/event-stream')


# Service code starts here
class SearchService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PythonConfluenceUnifiedSearch'
    _svc_display_name_ = 'Python Confluence Unified Search'
    # Add this line to specify the pythonservice.exe from your conda environment
    _exe_name_ = r"C:\ProgramData\Anaconda3\envs\confsearch\Scripts\pythonservice.exe"

    def __init__(self, args):
        logger.info("Service __init__()")
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True

    def SvcStop(self):
        logger.info("Service SvcStop()")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def SvcDoRun(self):
        logger.info("Service SvcDoRun()")
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.timeout = 5000
        self.main()

    def main(self):
        logger.info("STARTING - about to start confluence unified search")
        try:
            app.run(host='0.0.0.0', port=5051)
        except Exception as e:
            logger.exception("Exception in main service function")


def post_service_update(*args):
    import win32api, win32con, win32profile, pywintypes
    from contextlib import closing

    env_reg_key = "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment"
    hkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, env_reg_key, 0, win32con.KEY_ALL_ACCESS)

    with closing(hkey):
        system_path = win32api.RegQueryValueEx(hkey, 'PATH')[0]
        # PATH may contain %SYSTEM_ROOT% or other env variables that must be expanded
        # ExpandEnvironmentStringsForUser(None) only expands System variables
        system_path = win32profile.ExpandEnvironmentStringsForUser(None, system_path)
        system_path_list = system_path.split(os.pathsep)

        core_dll_file = win32api.GetModuleFileName(sys.dllhandle)
        core_dll_name = os.path.basename(core_dll_file)

        for search_path_dir in system_path_list:
            print("-" * 30)
            try:
                print(f"Search Path: {search_path_dir}")
                print(f"System python DLL: {core_dll_name}")
                dll_path = win32api.SearchPath(search_path_dir, core_dll_name)[0]
                print(f"System python DLL: {dll_path}")
                break
            except pywintypes.error as ex:
                if ex.args[1] != 'SearchPath': raise
                continue
        else:
            print("*** WARNING ***")
            print(f"Your current Python DLL ({core_dll_name}) is not in your SYSTEM PATH")
            print("The service is likely to not launch correctly.")

    from win32serviceutil import LocatePythonServiceExe
    pythonservice_exe = LocatePythonServiceExe()
    pywintypes_dll_file = pywintypes.__spec__.origin

    pythonservice_path = os.path.dirname(pythonservice_exe)
    pywintypes_dll_name = os.path.basename(pywintypes_dll_file)

    try:
        return win32api.SearchPath(pythonservice_path, pywintypes_dll_name)[0]
    except pywintypes.error as ex:
        if ex.args[1] != 'SearchPath': raise
        print("*** WARNING ***")
        print(f"{pywintypes_dll_name} is not is the same directory as pythonservice.exe")
        print(f'Copy "{pywintypes_dll_file}" to "{pythonservice_path}"')
        print("The service is likely to not launch correctly.")


if __name__ == '__main__':
    global DEBUG

    # Check if --DEBUG is passed as a command line argument
    if "--DEBUG" in sys.argv:
        print("Debug Mode (via command line)")
        DEBUG = True
        # Remove --DEBUG from argv to not interfere with service commands
        sys.argv.remove("--DEBUG")

        # Flask will print the URL when it starts
        app.run(debug=DEBUG, host='0.0.0.0', port=5052, use_reloader=False)
    # Check if running in an IDE debugger
    elif sys.gettrace() is None:
        print("Run Mode")
        DEBUG = False

        if len(sys.argv) == 1:
            print("argv==1")
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(SearchService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            print(f"Command line arguments: {sys.argv}")
            logging.info(f"Command line arguments: {sys.argv}")
            win32serviceutil.HandleCommandLine(SearchService, customOptionHandler=post_service_update)
    else:
        print("Debug Mode (via IDE)")
        DEBUG = True

        # Flask will print the URL when it starts
        app.run(debug=DEBUG, host='0.0.0.0', port=5052, use_reloader=False)
