#! /usr/bin/env sh
set -e

if [ -f ./.env ]; then
  export $(cat .env | xargs)
fi

echo "task running with env vars S3_BUCKET = $S3_BUCKET and S3_KEY = $S3_KEY"

DATABASE=${S3_KEY##*/}
DATABASE_NAME=${DATABASE%.*}

URL="https://$S3_BUCKET.s3.eu-west-2.amazonaws.com/$S3_KEY"

echo "downloading from $URL"
curl -O $URL

echo "load data into postgres"
python3 -m pgload.load --source=$DATABASE_NAME

echo "load done"
