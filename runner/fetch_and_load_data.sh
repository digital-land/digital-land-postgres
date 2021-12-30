#! /usr/bin/env sh
set -e

DATABASE=$1

echo "downloading $DATABASE"

curl -O "https://collection-dataset.s3.eu-west-2.amazonaws.com/$DATABASE-builder/dataset/$DATABASE.sqlite3"

sqlite3 $DATABASE.sqlite3 ".read export_$DATABASE.sql"

echo "export done - copy data"

export PGPASSWORD=postgres

psql -h localhost -f copy_$DATABASE.sql  -U postgres  digital_land

echo "copy done"

