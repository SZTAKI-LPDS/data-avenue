#!/bin/bash
set -x
. ./copy.config
DATA_AVENUE_HOST=localhost
DATA_AVENUE_PORT=8080
if [ "$#" -gt 0 ]; then DATA_AVENUE_HOST=$1 ; else echo Using default dataavenue host: localhost ; fi
KEY=dataavenue
if [ "$#" -gt 1 ]; then KEY=$2 ; else echo Using default dataavenue key: $KEY ; fi
CURL_OPTIONS='-k -f -sS'
S3_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: UserPass, UserID: ${ACCESS_KEY}, UserPass: ${SECRET_KEY}}"'

function copy {
  local ID=`eval curl $S3_HEADERS -X POST '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/$1"' '-H "Content-type: application/json"' '--data "{target:\"s3://$TARGET_S3_HOST/$TARGET_S3_BUCKET/\",overwrite:true,credentials:{Type:UserPass, UserID:\"$TARGET_ACCESS_KEY\", UserPass:\"$TARGET_SECRET_KEY\"}}"' https://${DATA_AVENUE_HOST}:8443/dataavenue/rest/transfers`
  echo Task id: $ID
  local TIMEOUT=60
  for retries in $(seq $TIMEOUT); do
    local STATUS=`eval curl $S3_HEADERS -X GET https://${DATA_AVENUE_HOST}:$DATA_AVENUE_PORT/dataavenue/rest/transfers/$ID`
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
  echo TIMEOUT: $ID \("${TIMEOUT}"s\)
}

copy 1MB.dat
