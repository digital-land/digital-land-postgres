#! /usr/bin/env sh
set -e

curl -O https://collection-dataset.s3.eu-west-2.amazonaws.com/entity-builder/dataset/entity.sqlite3

sqlite3 entity.sqlite3 ".read export.sql"

echo "export done - copy data"

export PGPASSWORD=postgres

psql -h localhost -f copy.sql  -U postgres  digital_land
echo "copy done"

