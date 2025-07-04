curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"getSpacePermissionSets\",\"params\":[\"EQEQGLOB\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"



curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"getSpacePermissionSet\",\"params\":[\"EQEQGLOB\",\"SETSPACEPERMISSIONS\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"

:: add as admin
curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"SETSPACEPERMISSIONS\",\"sconnor\",\"EQEQGLOB\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"

:: assign permissions
curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"EDITSPACE\",\"sconnor\",\"EQEQGLOB\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"


:: verify assigned
curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"getSpacePermissionSet\",\"params\":[\"EQEQGLOB\",\"SETSPACEPERMISSIONS\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"




curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"EDITSPACE\",\"confluence-users\",\"EQEQGLOB\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"


curl.exe --user "admin:admin" ^
  -H "Content-Type:application/json" ^
  -H "Accept:application/json" ^
  -X POST ^
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"addPermissionToSpace\",\"params\":[\"REMOVE_OWN_ATTACHMENTS\",\"jdoe\",\"EQEQGLOB\"],\"id\":7}" ^
  "http://192.168.65.128:8090/rpc/json-rpc/confluenceservice-v2?os_authType=basic"
