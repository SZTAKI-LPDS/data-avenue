#!/bin/bash
# set -x
# This script simulates load to DataAvenue server by sending a series of copy requests between source and target S3 storages

# -----------------------------------------------------------------------------------------------------------------
# config ----------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------

KEY=dataavenue
DATA_AVENUE_HOST=http://localhost:8080
DATA_AVENUE_SERVICE=$DATA_AVENUE_HOST/dataavenue/rest/transfers
DATA_AVENUE_FILES_SERVICE=$DATA_AVENUE_HOST/dataavenue/rest/files

# SOURCE, TARGET can be s3:// or sftp://
SOURCE_HOST=s3://....
SOURCE_BUCKET=source
SOURCE_ACCESS_KEY=myaccesskey
SOURCE_SECRET_KEY=***

TARGET_HOST=sftp://...
TARGET_BUCKET=tmp
TARGET_ACCESS_KEY=myusername
TARGET_SECRET_KEY=***

# file to copy from SOURCE (must be available)
FILE_TO_COPY=100mb.dat

# -----------------------------------------------------------------------------------------------------------------
# utils -----------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------

CURL_OPTIONS='-k -f -sS'
S3_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type:UserPass,UserID:${SOURCE_ACCESS_KEY},UserPass:${SOURCE_SECRET_KEY}}"'
TARGET_CREDENTIALS="{Type:UserPass,UserID:"${TARGET_ACCESS_KEY}",UserPass:"${TARGET_SECRET_KEY}"}"

# synchronous copy (wait for completion, then return)
# parameters:
# $1: source file name
# $2: target file name (optional)
function copy_sync() {
  local SOURCE_FILE=$1
  local TARGET_FILE=""
  if [ $# -gt 1 ]; then TARGET_FILE=$2; fi
  local ID=`eval curl $S3_HEADERS -X POST '-H "Content-type: application/json"' '-H "x-uri: $SOURCE_HOST/$SOURCE_BUCKET/$SOURCE_FILE"' '--data "{target:\"$TARGET_HOST/$TARGET_BUCKET/$TARGET_FILE\",overwrite:true,credentials:{Type:UserPass, UserID:\"$TARGET_ACCESS_KEY\", UserPass:\"$TARGET_SECRET_KEY\"}}"' ${DATA_AVENUE_SERVICE}`
  # echo Task id: $ID
  local TIMEOUT=60
  for retries in $(seq $TIMEOUT); do
    local STATUS=`eval curl $S3_HEADERS ${DATA_AVENUE_SERVICE}/$ID`
    if [[ $STATUS == *"DONE"* ]]; then
      echo DONE: $ID
      # echo $STATUS
      return;
    elif [[ $STATUS == *"FAILED"* ]]; then
      echo FAILED: $ID
      # echo $STATUS
      return;
    else
      # echo TRANSFERRING: $ID
      sleep 1
    fi
  done
  echo TIMEOUT: $ID \("${TIMEOUT}"s\)
}

# asynchronous copy (just start, don't wait for completion)
# parameters:
# $1: source file name
# $2: target file name (optional)
function copy_async() {
  local SOURCE_FILE=$1
  local TARGET_FILE=""
  if [ $# -gt 1 ]; then TARGET_FILE=$2; fi;
  local ID=`eval curl $S3_HEADERS -X POST '-H "Content-type: application/json"' '-H "x-uri: $SOURCE_HOST/$SOURCE_BUCKET/$SOURCE_FILE"' '--data "{target:\"$TARGET_HOST/$TARGET_BUCKET/$TARGET_FILE\",overwrite:true,credentials:{Type:UserPass, UserID:\"$TARGET_ACCESS_KEY\", UserPass:\"$TARGET_SECRET_KEY\"}}"' ${DATA_AVENUE_SERVICE}`
  echo Copy task started: $ID
}

# deletes file on target host
# parameters:
# $1: file name to delete
function delete() {
  # echo Deleting file $1
  local TARGET_FILE=$1
  curl $CURL_OPTIONS -H "x-key: ${KEY}" -H "x-credentials: $TARGET_CREDENTIALS" -X DELETE -H "x-uri: $TARGET_HOST/$TARGET_BUCKET/$TARGET_FILE" ${DATA_AVENUE_FILES_SERVICE}
  if [ $? -eq 0 ]; then echo "Deleted: $1"; else echo "Error at deleting $1"; fi
}

# -----------------------------------------------------------------------------------------------------------------
# load scenario ---------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------

# start N copy with delay of D seconds
N=10
D=1

# sync: copy one-by-one
#for i in $(seq $N); do
#  echo Strarting transfer of $FILE_TO_COPY
#  copy_sync $FILE_TO_COPY $i.dat
#  sleep $D
#done

# async start all at once
for i in $(seq $N); do
  echo Strarting transfer of $FILE_TO_COPY
  copy_async $FILE_TO_COPY $i.dat
done

# do any scenario below with sleeps, arbitratry number of transfers
# ...

# cleanup
#for i in $(seq $N); do
#  delete $i.dat
#done