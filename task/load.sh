#! /usr/bin/env bash

DATABASE=${S3_KEY##*/}
DATABASE_NAME=${DATABASE%.*}

echo "$EVENT_ID: running with settings: S3_BUCKET=$S3_BUCKET, S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"

if ! [ -f "$DATABASE_NAME.sqlite3" ]; then
  echo "$EVENT_ID: attempting download from s3://$S3_BUCKET/$S3_KEY"
  aws s3api get-object --bucket "$S3_BUCKET" --key "$S3_KEY" "$DATABASE_NAME.sqlite3" || \
    echo "$EVENT_ID: failed to download from s3://$S3_BUCKET/$S3_KEY" && exit 1

  echo "$EVENT_ID: finished downloading from s3://$S3_BUCKET/$S3_KEY"
fi

echo "$EVENT_ID: extracting data from $DATABASE"
sqlite3 "$DATABASE" ".read sql/export_$DATABASE_NAME.sql" || \
  echo "$EVENT_ID: failed to extract data from $DATABASE" && exit 1

echo "$EVENT_ID: loading data into postgres"
python3 -m pgload.load --source="$DATABASE_NAME" || \
  echo "$EVENT_ID: failed to load $DATABASE" && exit 1

echo "$EVENT_ID: loading of $DATABASE_NAME completed successfully"
