#!/bin/bash
# Usage: test.sh [dataavenue host] [dataavenue key]
#set -x

# load credentials, set constants
. ./copy.config
DATA_AVENUE_HOST=http://localhost:8080
if [ "$#" -gt 0 ]; then DATA_AVENUE_HOST=$1 ; else echo Using default dataavenue host: localhost ; fi
KEY=dataavenue
if [ "$#" -gt 1 ]; then KEY=$2 ; else echo Using default dataavenue key: $KEY ; fi
CURL_OPTIONS='-k -f -sS -o /dev/null'
function check() { if [ $? -eq 0 ]; then echo '  'PASSED: $1 ; else echo '  'FAILED: $1 ; fi }

# test version
echo Testing Data Avenue version...
curl $CURL_OPTIONS ${DATA_AVENUE_HOST}/dataavenue/rest/version ; check "version"

# test SFTP
if [ -n "${SFTP_HOST}" ] && [ -n "${SFTP_USERNAME}" ] && [ -n "${SFTP_PASSWORD}" ]; then
  echo Testing SFTP...
  SFTP_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: UserPass, UserID: ${SFTP_USERNAME}, UserPass: ${SFTP_PASSWORD}}"'
  eval curl $SFTP_HEADERS -X GET    '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/"'            ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "sftp list"
  eval curl $SFTP_HEADERS -X POST   '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "sftp mkdir"
  eval curl $SFTP_HEADERS -X GET    '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "sftp dir attributes"
  eval curl $SFTP_HEADERS -X POST   '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/test" -H "Content-Type: application/octet-stream" --data-binary @test.sh' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "sftp upload"
  eval curl $SFTP_HEADERS -X GET    '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "sftp file attributes"
  eval curl $SFTP_HEADERS -X GET    '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "sftp download"
  eval curl $SFTP_HEADERS -X DELETE '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "sftp delete"
  eval curl $SFTP_HEADERS -X DELETE '-H "x-uri: sftp://$SFTP_HOST$SFTP_DIR/newdir/"'     ${DATA_AVENUE_HOST}dataavenue/rest/directory ; check "sftp rmdir"
else 
  echo Skipping SFTP tests \(not configured\)
fi

# test S3
if [ -n "${S3_HOST}" ] && [ -n "${ACCESS_KEY}" ] && [ -n "${SECRET_KEY}" ]; then
  echo Testing S3...
  S3_HEADERS=$CURL_OPTIONS' -H "x-key: ${KEY}" -H "x-credentials: {Type: UserPass, UserID: ${ACCESS_KEY}, UserPass: ${SECRET_KEY}}"'
  eval curl $S3_HEADERS -X GET    '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/"'            ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "s3 list"
  eval curl $S3_HEADERS -X POST   '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "s3 mkdir"
  eval curl $S3_HEADERS -X GET    '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "s3 dir attributes"
  eval curl $S3_HEADERS -X POST   '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/test" -H "Content-Type: application/octet-stream" --data-binary @test.sh' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "s3 upload"
  eval curl $S3_HEADERS -X GET    '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/attributes ; check "s3 file attributes"
  eval curl $S3_HEADERS -X GET    '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "s3 download"
  eval curl $S3_HEADERS -X DELETE '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/test"' ${DATA_AVENUE_HOST}/dataavenue/rest/file ; check "s3 delete"
  eval curl $S3_HEADERS -X DELETE '-H "x-uri: s3://$S3_HOST/$S3_BUCKET/newdir/"'     ${DATA_AVENUE_HOST}/dataavenue/rest/directory ; check "s3 rmdir"
else 
  echo Skipping S3 tests \(not configured\)
fi

echo Done
