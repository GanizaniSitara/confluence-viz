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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


app = Flask(__name__)

logging.basicConfig(filename=r"D:\solutions\Python\confluence_unified_search\confluence_search_service.log",level=logging.WARNING,
                    format='%(asctime)s %(levelname)s %(message)s')

# https://stackoverflow.com/questions/24344045/how-can-i-completely-remove-any-logging-from-requests-module-in-python
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").propagate = False
logging.getLogger("requests").setLevel(logging.CRITICAL)

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'CONFLUENCE_URL': config['confluence']['base_url'],
        'USERNAME': config['credentials']['USERNAME'],
        'PASSWORD': config['credentials']['PASSWORD']
    }

config = load_config()

confluence = Confluence(
    url=config['CONFLUENCE_URL'],
    username=config['USERNAME'],
    password=config['PASSWORD'],
    verify_ssl=False
)

message_queue=[]

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
                         result['resultGlobalContainer']['displayUrl'].replace('/display/', ''))].append(result["lastModified"][:10])
        except KeyError as e:
            print(f"ERROR {e}")
            print(f"ERROR {result}")

    return space_dates
#
# def create_space_link(space_name, space_key,cql,three_years):
#     if not three_years:
#         url = (f"https://confluence.example.com/dosearchsite.action?cql=siteSearch%20~%20%22{urllib.parse.quote_plus(cql)}"
#                f"%22%20AND%20space%20in%20(%22{space_key}%22)%20AND%20type%20in%20(%22page%22)&includeArchivedSpaces=false")
#     else:
#         url = (f"https://confluence.example.com/dosearchsite.action?cql=siteSearch+~+%22{urllib.parse.quote_plus(cql)}"
#                f"%22+and+space+%3D+%22{space_key}%22+and+lastmodified+%3E%3D+%222021-01-01%22+and+type+%3D+%22page%22&queryString={urllib.parse.quote_plus(cql)}")
#     return f'<a href="{url}">{space_name}</a>'


@app.route('/', methods=['GET', 'POST'])
def search_confluence():
    if request.method == 'POST':
        cql_query = request.form['cql_query']

        #if contains_quotes(cql_query):
        #    return {"error": "Quotes are not supported in search queries. Please remove any quotation marks and try again."}

        if cql_query == "":
            return render_template("search.html")

        # this is where we deal with the quotes in input, lets' see
        cql_query = escape_quotes_in_cql(cql_query)

        cql_query_hist = cql_query + '" and lastmodified > 2020-01-01'
        cql = 'text~"' + cql_query + '" and type=page'
        cql_hist = 'text~"' + cql_query_hist + ' and type=page'

        print(cql)


        all_history_page_dates = get_search_results(cql)

        timelines=[]
        now = datetime.now()
        years = 10
        years_ago = now - timedelta(days=years * 365)

        temp_list = list(all_history_page_dates.items())
        temp_list.sort(key=lambda x: len(x[1]), reverse=True)

        sorted_dict = defaultdict(list)
        for key, value in temp_list:
            sorted_dict[key].extend(value)

        for key, values in sorted_dict.items():
            events = [(datetime.fromisoformat(date) - years_ago) / (now - years_ago) if (datetime.fromisoformat(date) - years_ago) / (now - years_ago) > 0 else 0 for date in values]
            count = len(events)
            desc = key[0]
            url = (f"{config['CONFLUENCE_URL']}/dosearchsite.action?cql=siteSearch%20~%20%22{urllib.parse.quote_plus(cql_query)}"
                   f"%22%20AND%20space%20in%20(%22{key[1]}%22)%20AND%20type%20in%20(%22page%22)&includeArchivedSpaces=false")

            print(url)

            timelines.append((events, desc, url, count))


        return render_template("timeline.html", timelines=timelines, now=now,cql=cql_query)

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
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys


class SearchService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PythonConfluenceUnifiedSearch'
    _svc_display_name_ = 'Python Confluence Unified Search'

    def __init__(self, args):
        logging.info("__init__()")
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def SvcDoRun(self):
        logging.info("SvcDoRun()")
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.timeout = 5000
        self.main()

    def main(self):
        logging.info("STARTING - about to start confluence unified search")
        try:
            app.run(host='0.0.0.0',port=5051)
        except Exception as e:
            logging.exception(e)


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