#!/bin/bash
# Usage: test.sh [dataavenue host] [dataavenue key]
#set -x

# load credentials, set constants
. ./test.config
DATA_AVENUE_HOST=http://localhost:8080
if [ "$#" -gt 0 ]; then DATA_AVENUE_HOST=$1 ; else echo Using default dataavenue host: localhost ; fi
KEY=dataavenue-key
if [ "$#" -gt 1 ]; then KEY=$2 ; else echo Using default dataavenue key: $KEY ; fi
CURL_OPTIONS='-k -f -sS -o /dev/null'
function check() { if [ $? -eq 0 ]; then echo '  'PASSED: $1 ; else echo '  'FAILED: $1 ; fi }

# test version
echo Testing Data Avenue version...
curl $CURL_OPTIONS ${DATA_AVENUE_HOST}/dataavenue/rest/version ; check "version"

# test Google drive
if [  -n "${GOOGLEDRIVE_HOST}" ] && "${GOOGLEDRIVE_ACCESS_KEY}" ]; then
  echo Testing Google Drive...
  GOOGLEDRIVE_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: accessKey, accessKey: ${GOOGLEDRIVE_ACCESS_KEY}}"'
  eval curl $GOOGLEDRIVE_HEADERS -X GET    '-H "x-uri: googledrive://googledrive.com/"'            ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "googledrive list"
  eval curl $GOOGLEDRIVE_HEADERS -X POST   '-H "x-uri: googledrive://googledrive.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "googledrive mkdir"
  eval curl $GOOGLEDRIVE_HEADERS -X POST   '-H "x-uri: googledrive://googledrive.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "googledrive dir attributes"
  eval curl $GOOGLEDRIVE_HEADERS -X POST   '-H "x-uri: googledrive://googledrive.com/newdir/test" -H "Content-Type: application/octet-stream" --data-binary @test.sh' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "googledrive upload"
  eval curl $GOOGLEDRIVE_HEADERS -X GET    '-H "x-uri: googledrive://googledrive.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "googledrive file attributes"
  eval curl $GOOGLEDRIVE_HEADERS -X GET    '-H "x-uri: googledrive://googledrive.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "googledrive download"
  eval curl $GOOGLEDRIVE_HEADERS -X DELETE '-H "x-uri: googledrive://googledrive.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "googledrive delete"
  eval curl $GOOGLEDRIVE_HEADERS -X DELETE '-H "x-uri: googledrive://googledrive.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "googledrive rmdir"
else 
  echo Skipping GOOGLE DRIVE tests \(not configured\)
fi

echo Done
