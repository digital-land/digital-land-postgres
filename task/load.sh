#! /usr/bin/env bash

echo "task running with env vars S3_BUCKET = $S3_BUCKET and S3_KEY = $S3_KEY"

DATABASE=${S3_KEY##*/}
DATABASE_NAME=${DATABASE%.*}

if ! [ -f $DATABASE_NAME.sqlite3 ]; then
  echo "$EVENT_ID: attempting download from $COLLECTION_DATA_URL/$S3_KEY"
  aws s3api get-object --bucket $S3_BUCKET --key $S3_KEY $DATABASE_NAME.sqlite3 || \
    echo "$EVENT_ID: failed to download from s3://$S3_BUCKET/$S3_KEY" && exit 1

  echo "$EVENT_ID: finished downloading from s3://$S3_BUCKET/$S3_KEY"
fi

echo "$EVENT_ID: exporting $DATABASE"
sqlite3 $DATABASE ".read sql/export_$DATABASE_NAME.sql" || echo "$EVENT_ID: failed to export $DATABASE"

echo "$EVENT_ID: load data into RDS postgres"
python3 -m pgload.load --source=$DATABASE_NAME || echo "$EVENT_ID: failed to load $DATABASE"

echo "$EVENT_ID: loading of database $DATABASE_NAME completed successfully"
