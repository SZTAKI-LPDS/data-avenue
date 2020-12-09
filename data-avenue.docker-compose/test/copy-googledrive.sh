#!/bin/bash
#set -x
. ./copy.config
DATA_AVENUE_HOST=localhost
DATA_AVENUE_PORT=8080
if [ "$#" -gt 0 ]; then DATA_AVENUE_HOST=$1 ; else echo Using default dataavenue host: localhost ; fi
KEY=dataavenue-key
if [ "$#" -gt 1 ]; then KEY=$2 ; else echo Using default dataavenue key: $KEY ; fi
CURL_OPTIONS='-k -f -sS'
GOOGLEDRIVE_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: accessKey, accessKey: ${GOOGLEDRIVE_ACCESS_KEY}}"'

function copy {
  local ID=`eval curl $GOOGLEDRIVE_HEADERS -X POST '-H "x-uri: googledrive://$GOOGLEDRIVE_HOST/$GOOGLEDRIVE_SOURCE_TEST_DIR/"' '-H "Content-type: application/json"' '--data "{target:\"dropbox://$DROPBOX_HOST/$GOOGLEDRIVE_TAERGET_TEST_DIR/\",overwrite:true,credentials: {Type:accessToken, accessToken:\"$DROPBOX_HOST_ACCESS_TOKEN\"}}"' http://${DATA_AVENUE_HOST}:${DATA_AVENUE_PORT}/dataavenue/rest/transfers`
  echo Task id: $ID
  local TIMEOUT=60
  for retries in $(seq $TIMEOUT); do
    local STATUS=`eval curl $GOOGLEDRIVE_HEADERS -X GET http://${DATA_AVENUE_HOST}:{$DATA_AVENUE_PORT}/dataavenue/rest/transfers/$ID`
    if [[ $STATUS == *"DONE"* ]]; then
      echo DONE: $ID
      echo $STATUS
      return;
    elif [[ $STATUS == *"FAILED"* ]]; then
      echo FAILED: $ID
      echo $STATUS
      return;
    else
      # echo TRANSFERRING: $ID
      sleep 1
    fi
  done
  echo TIMEOUT: $ID \("${T\)IMEOUT}"s
}

copy 1MB.dat