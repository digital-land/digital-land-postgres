#! /usr/bin/env sh
# Environment Variables:
## Required for all destinations:
# * S3_KEY
# * S3_BUCKET
## Required for RDS:
# * DB_NAME
# * DB_USER_NAME
# * DB_PASSWORD
# * DB_WRITE_ENDPOINT
## Required for CloudFoundry:
# * CF_USER
# * CF_PASSWORD
# * ENVIRONMENT
# * CF_DATABASE_SERVICE_NAME
set -e

if [ -f ./.env ]; then
  export $(cat .env | xargs)
fi

echo "task running with env vars S3_BUCKET = $S3_BUCKET and S3_KEY = $S3_KEY"

DATABASE=${S3_KEY##*/}
DATABASE_NAME=${DATABASE%.*}

URL="https://$S3_BUCKET.s3.eu-west-2.amazonaws.com/$S3_KEY"

if ! [ -f $DATABASE_NAME.sqlite3 ]; then
  echo "downloading from $URL"
  curl -O $URL
fi

echo "exporting $DATABASE"
sqlite3 $DATABASE ".read sql/export_$DATABASE_NAME.sql"

if [ -n "$CF_DATABASE_SERVICE_NAME" ]; then
  echo "load data into gov.uk PaaS postgres"
  # shellcheck disable=SC2016
  cf login -a api.london.cloud.service.gov.uk -u $CF_USER -p "$CF_PASSWORD" -s "$ENVIRONMENT"
  # Cloudfoundry conduit addon does some magic where it executes a process on your host machine that has access a particular cloudfoundry hosted service
  cf conduit "$CF_DATABASE_SERVICE_NAME" -- bash -c "python3 -m pgload.load_with_vcap_credentials --source=$DATABASE_NAME"
else
  echo "load data into RDS postgres"
  python3 -m pgload.load --source=$DATABASE_NAME

  if [ $DATABASE_NAME == "entity" ]; then
    echo "loading facts"
    python3 -m pgload.load_facts
  fi

fi

echo "load done"
