#! /usr/bin/env sh
set -e

echo "task running with env vars S3_COLLECTION_BUCKET = $S3_COLLECTION_BUCKET and S3_KEY = $S3_KEY"

DATABASE=${S3_KEY##*/}
DATABASE_NAME=${DATABASE%.*}

URL="$S3_COLLECTION_BUCKET/$S3_KEY"
echo "downloading from $URL"
curl -O $URL

echo "exporting $DATABASE to exported_$DATABASE_NAME.csv"
sqlite3 $DATABASE ".read sql/export_$DATABASE_NAME.sql"

echo "load data from exported_$DATABASE_NAME.csv into postgres"
python3 -m pgload.load --csvfile=exported_$DATABASE_NAME.csv --source=$DATABASE_NAME

echo "load done"
