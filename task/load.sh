#! /usr/bin/env bash
# need to use the files cdn instead of the bucket name when loading locally without logging into aws

s3_object_arn_regex="^arn:aws:s3:::([0-9A-Za-z-]*/)(.*)$"

if ! [[ "$S3_OBJECT_ARN" =~ $s3_object_arn_regex ]]; then
    echo "Received invalid S3 Object S3 ARN: $S3_OBJECT_ARN, skipping"
    exit 1
fi

S3_BUCKET=${BASH_REMATCH[1]%/*}
S3_KEY=${BASH_REMATCH[2]}

DATABASE=${S3_KEY##*/}
export DATABASE_NAME=${DATABASE%.*}
echo "DATABASE NAME: $DATABASE_NAME"
echo "$EVENT_ID: running with settings: S3_BUCKET=$S3_BUCKET, S3_KEY=$S3_KEY, DATABASE=$DATABASE, DATABASE_NAME=$DATABASE_NAME"
# if [[ $DATABASE_NAME != "entity" && $DATABASE_NAME != "digital-land" ]]; then
#   echo "$EVENT_ID: wrong database, skipping"
#   exit 1
# fi

# download specification
export SOURCE_URL=https://raw.githubusercontent.com/digital-land/
mkdir -p specification/
curl -qfsL $SOURCE_URL/specification/main/specification/attribution.csv > specification/attribution.csv
curl -qfsL $SOURCE_URL/specification/main/specification/licence.csv > specification/licence.csv
curl -qfsL $SOURCE_URL/specification/main/specification/typology.csv > specification/typology.csv
curl -qfsL $SOURCE_URL/specification/main/specification/theme.csv > specification/theme.csv
curl -qfsL $SOURCE_URL/specification/main/specification/collection.csv > specification/collection.csv
curl -qfsL $SOURCE_URL/specification/main/specification/dataset.csv > specification/dataset.csv
curl -qfsL $SOURCE_URL/specification/main/specification/dataset-field.csv > specification/dataset-field.csv
curl -qfsL $SOURCE_URL/specification/main/specification/field.csv > specification/field.csv
curl -qfsL $SOURCE_URL/specification/main/specification/datatype.csv > specification/datatype.csv
curl -qfsL $SOURCE_URL/specification/main/specification/prefix.csv > specification/prefix.csv
# deprecated ..
curl -qfsL $SOURCE_URL/specification/main/specification/pipeline.csv > specification/pipeline.csv
curl -qfsL $SOURCE_URL/specification/main/specification/dataset-schema.csv > specification/dataset-schema.csv
curl -qfsL $SOURCE_URL/specification/main/specification/schema.csv > specification/schema.csv
curl -qfsL $SOURCE_URL/specification/main/specification/schema-field.csv > specification/schema-field.csv


if ! [ -f "$DATABASE_NAME.sqlite3" ]; then
  echo "$EVENT_ID: attempting download from s3://$S3_BUCKET/$S3_KEY"
  aws s3api get-object --bucket "$S3_BUCKET" --key "$S3_KEY" "$DATABASE_NAME.sqlite3" > /dev/null
  if ! [ -f "$DATABASE_NAME.sqlite3" ]; then
    echo "$EVENT_ID: failed to download from s3://$S3_BUCKET/$S3_KEY"
    exit 1
  else
    echo "$EVENT_ID: finished downloading from s3://$S3_BUCKET/$S3_KEY"
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

  # If exported_old_entity is blank, check there are no records in the sqlite database.
  # If there aren't, then replace the file with the headers, so it will be processed.
  if [ ! -s exported_old_entity.csv ]; then
    COUNT=`sqlite3 $DATABASE 'SELECT COUNT(*) FROM old_entity'`
    if [ "$COUNT" -eq "0" ]; then
      echo "entity|old_entity|entry_date|start_date|end_date|status|notes|dataset" > exported_old_entity.csv
    fi
  fi

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
