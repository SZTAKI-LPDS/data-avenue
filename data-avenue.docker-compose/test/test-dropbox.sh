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

# test Dropbox
if [ -n "${DROPBOX_HOST}" ] && [ -n "${DROPBOX_ACCESS_TOKEN}" ]; then
  echo Testing Dropbox...
  DROPBOX_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: accessToken, accessToken: ${DROPBOX_ACCESS_TOKEN}}"'
  eval curl $DROPBOX_HEADERS -X GET    '-H "x-uri: dropbox://dropbox.com/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "dropbox list"
  eval curl $DROPBOX_HEADERS -X POST   '-H "x-uri: dropbox://dropbox.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "dropbox mkdir"
  eval curl $DROPBOX_HEADERS -X POST   '-H "x-uri: dropbox://dropbox.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ;  check "dropbox dir attributes"
  eval curl $DROPBOX_HEADERS -X POST   '-H "x-uri: dropbox://dropbox.com/newdir/test" -H "Content-Type: application/octet-stream" --data-binary @test.sh' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "dropbox upload"
  eval curl $DROPBOX_HEADERS -X GET    '-H "x-uri: dropbox://dropbox.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "dropbox file attributes"
  eval curl $DROPBOX_HEADERS -X GET    '-H "x-uri: dropbox://dropbox.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "dropbox download"
  eval curl $DROPBOX_HEADERS -X DELETE '-H "x-uri: dropbox://dropbox.com/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "dropbox delete"
  eval curl $DROPBOX_HEADERS -X DELETE '-H "x-uri: dropbox://dropbox.com/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "dropbox rmdir"
else 
  echo Skipping DROPBOX tests \(not configured\)
fi


echo Done
