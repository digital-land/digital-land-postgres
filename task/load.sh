#! /usr/bin/env sh
set -e

if [ "$#" -eq 0 ]
then
    echo "run ./load.sh with arguments [entity or digital-land or both]"
    exit 1
fi

for DATABASE in "$@"
do
    echo "loading $DATABASE"
    URL="https://collection-dataset.s3.eu-west-2.amazonaws.com/$DATABASE-builder/dataset/$DATABASE.sqlite3"

    echo "downloading from $URL"
    curl -O $URL

    echo "exporting from $DATABASE.sqlite3"
    sqlite3 $DATABASE.sqlite3 ".read sql/export_$DATABASE.sql"

    echo "export done - load data from exported_$DATABASE.csv"
    python3 -m pgload.load --csvfile=exported_$DATABASE.csv --source=$DATABASE

    echo "load done"
done

