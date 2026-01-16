
set SERVICE_SCRIPT=search_aggregation_service.py
net stop PythonConfluenceUnifiedSearch >nul 2>&1
python %SERVICE_SCRIPT% remove >nul 2>&1
python %SERVICE_SCRIPT% install
net start PythonConfluenceUnifiedSearch

