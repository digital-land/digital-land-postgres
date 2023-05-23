#! /usr/bin/env bash
# need to use the files cdn instead of the bucket name when loading locally without logging into aws
DATABASE=${S3_KEY##*/}
export DATABASE_NAME=${DATABASE%.*}
echo "DATABASE NAMW: $DATABASE_NAME"
echo "$EVENT_ID: running with settings: S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"



# if [[ $DATABASE_NAME != "entity" && $DATABASE_NAME != "digital-land" ]]; then
#   echo "$EVENT_ID: wrong database, skipping"
#   exit 1
# fi


if ! [ -f "$DATABASE_NAME.sqlite3" ]; then
  echo "$EVENT_ID: attempting download from https://files.planning.data.gov.uk/$S3_KEY"
  if curl --fail --show-error --location "https://files.planning.data.gov.uk/$S3_KEY" > "$DATABASE_NAME.sqlite3"; then
      echo "$EVENT_ID: finished downloading from https://files.planning.data.gov.uk/$S3_KEY"
  else
      echo "$EVENT_ID: failed to download from https://files.planning.data.gov.uk/$S3_KEY"
      rm "$DATABASE_NAME.sqlite3"  # remove the file if it was created
      exit 1
  fi
fi





if [[ $DATABASE_NAME == "digital-land" ]]; then
  echo "$EVENT_ID: extracting data from $DATABASE"
  sqlite3 "$DATABASE" ".read sql/export_$DATABASE_NAME.sql"
  if ! [ -f exported_attribution.csv ] \
  || ! [ -f exported_dataset.csv ] \
  || ! [ -f exported_dataset_collection.csv ] \
  || ! [ -f exported_dataset_publication.csv ] \
  || ! [ -f exported_licence.csv ] \
  || ! [ -f exported_lookup.csv ] \
  || ! [ -f exported_organisation.csv ] \
  || ! [ -f exported_typology.csv ]; then
    echo "$EVENT_ID: failed to extract data from $DATABASE"
    exit 1
  fi


elif [[ $DATABASE_NAME != "entity" ]]; then
  echo "$EVENT_ID: extracting entity data from $DATABASE"
  cat sql/export_entity.sql | sed "s/\${DATABASE_NAME}/$DATABASE_NAME/g" | sqlite3 "$DATABASE"

  if ! [ -f exported_entity.csv ] || ! [ -f exported_old_entity.csv ]; then
    echo "$EVENT_ID: failed to extract data from $DATABASE"
    exit 1
  fi
fi

echo "$EVENT_ID: successfully extracted data from $DATABASE"

echo "$EVENT_ID: loading data into postgres"
python3 -m pgload.load --source="$DATABASE_NAME" || \
  (echo "$EVENT_ID: failed to load $DATABASE" && exit 1)

echo "$EVENT_ID: loading of $DATABASE_NAME completed successfully"