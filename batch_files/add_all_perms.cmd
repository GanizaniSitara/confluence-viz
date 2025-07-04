set USER=admin
set PASS=admin
set SPACE=EQEQGLOB
set TARGET=sconnor
set URL=http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic

curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"VIEWSPACE\",\"%TARGET%\",\"%SPACE%\"],\"id\":1}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVEOWNCONTENT\",\"%TARGET%\",\"%SPACE%\"],\"id\":2}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"COMMENT\",\"%TARGET%\",\"%SPACE%\"],\"id\":3}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"EDITSPACE\",\"%TARGET%\",\"%SPACE%\"],\"id\":4}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"SETSPACEPERMISSIONS\",\"%TARGET%\",\"%SPACE%\"],\"id\":5}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVEPAGE\",\"%TARGET%\",\"%SPACE%\"],\"id\":6}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVECOMMENT\",\"%TARGET%\",\"%SPACE%\"],\"id\":7}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVEBLOG\",\"%TARGET%\",\"%SPACE%\"],\"id\":8}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"CREATEATTACHMENT\",\"%TARGET%\",\"%SPACE%\"],\"id\":9}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVEATTACHMENT\",\"%TARGET%\",\"%SPACE%\"],\"id\":10}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"EDITBLOG\",\"%TARGET%\",\"%SPACE%\"],\"id\":11}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"EXPORTSPACE\",\"%TARGET%\",\"%SPACE%\"],\"id\":12}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVEMAIL\",\"%TARGET%\",\"%SPACE%\"],\"id\":13}" "%URL%"
curl.exe --user "%USER%:%PASS%" -H "Content-Type:application/json" -H "Accept:application/json" -X POST -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"SETPAGE_PERMISSIONS\",\"%TARGET%\",\"%SPACE%\"],\"id\":14}" "%URL%"
