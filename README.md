# Load data into digital-land.info postgres database

## Loading data into postgres in AWS

This repository contains code that is used as a runnable task in ECS. The
entry point [task/load.sh](task/load.sh) expects an environment variable
is set for the S3 object to consume:

    S3_OBJECT_ARN=arn:aws:s3:::some-bucket/some-key.sqlite3

These provide a bucket and key to load data from. At the moment the keys are assumed to be sqlite files produced by
the digital land collection process.

The task is triggered by an S3 put object event tracked by AWS Cloudtrail which extracts event metadata for the S3
bucket and key name and provides those to this container as enviroment variables.

The setup of this is in the [Tasks module](https://github.com/digital-land/digital-land-infrastructure/tree/main/terraform/modules/tasks)
of the digital-land-terraform repository.

To see how the values for bucket and key are extracted have a [look here](https://github.com/digital-land/digital-land-infrastructure/blob/main/terraform/modules/tasks/main.tf#L136:L155)

## Running locally to load data into local postgres

Running locally does not download the Digital Land Sqlite database from S3 directly but instead via a CDN, it is
necessary to ensure the $S3_OBJECT_ARN contains the correct file path. The bucket name portion of the ARN will
be ignored and the file path will be appended to https://files.planning.data.gov.uk/.

**Prerequisites**

   - A running postgres server (tested with PostgreSQL 14)
   - curl
   - sqlite

The assumption is that the target digital_land db already has entity and dataset tables. In other words migrations
from the [digital-land.info](https://github.com/digital-land/digital-land.info) repository should have been run against
the postgres database you want to load data into (the postgres database used by you locally running digital-land.info web
application)

### Steps to load data

1. **Copy the file [task/.env.example](task/.env.example) to [task/.env](task/.env)**

With a fresh checkout that file configures the scripts in this repo to load the digital-land database.

To load the entity database ensure the S3_OBJECT_ARN has the correct key for the entity sqlite database (see below).


2. **Create a virtualenv and install requirements**

cd into the task directory and run:

    pip install -r requirements.txt

3. **Run the load script in task directory to load digital-land**

Remember the .env file is already set to load the digital-land db. However in order to load the db without using an aws account sign in you will need to use a different script

    source .env && ./load_local.sh

6. **Run the load script to load entities from a specific dataset database**

Update the S3_OBJECT_ARN in the .env file to S3_OBJECT_ARN=arn:aws:s3:::digital-land-production-collection-dataset/[collection-name]-collection/dataset/[dataset-name].sqlite3

For example to load ancient-woodland-status, update the .env to contain the following:

    export S3_OBJECT_ARN=arn:aws:s3:::digital-land-production-collection-dataset/ancient-woodland-collection/dataset/ancient-woodland-status.sqlite3

The run:

    source .env && ./load_local.sh

You'll notice that the load script downloads sqlite databases and creates csv files in the directory it runs from. These
files are git and docker ignored, so once done loading you can delete. It's a dumb script so each time you run it
the files get downloaded/created again.

## Adding the 'meta data' to the local database
this includes things like the datasets and other meta data that is not included in the collections

1. replace the env variable with digital land builder like so:
```
    ./load_local.sh
```
2. update env
```
    source .env
```
3. run the load local sh
```
    ./load_local.sh
```
