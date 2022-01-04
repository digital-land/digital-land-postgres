# Manage digital land postgres


## Loading data into postgres in AWS

This repoistory contains code that is used as a runnable task in ECS. The
entry point [task/load.sh](tast/load.sh) expects environment variables
are set for S3_COLLECTION_BUCKET and S3_KEY that provide a bucket and key
to load data from. At the moment they keys are assumed to be sqlite files
produced by the digital  land collection process.

The task is triggered by an S3 put object event tracked by AWS Cloudtrail
which extracts event metadata for the S3 bucket and key name and provides
those to this container as enviroment variables. The setup of this is in the
[Tasks module](https://github.com/digital-land/digital-land-infrastructure/tree/main/terraform/modules/tasks)
of the digital-land-terraform repository.

To see how the values for bucket and key are extracated have a [look here](https://github.com/digital-land/digital-land-infrastructure/blob/main/terraform/modules/tasks/main.tf#L136:L155)


## Running this locally to load data into local postgres

**Prerequisites**

A running postgres server (tested with PostgreSQL 13)

Assumption is target digital_land db already has entity and dataset tables. In other words
migrations are up to date.

There is a docker compose file to help in using this code locally. 

_**If you have postgres up and running on host machine and you don't need or don't want to use docker compose, 
then just set the env variables listed bekiw and run load.sh in the task directory.**_

If you want to use docker-compose then read on.

First build the image.

    docker-compose build

then to load digital-land sqlite database set the following env variables (use a .env file, there's an .env.example in this repo):

    S3_COLLECTION_BUCKET=https://collection-dataset.s3.eu-west-2.amazonaws.com
    S3_KEY=digital-land-builder/dataset/digital-land.sqlite3

and run:

    docker-compose run --rm task

or to load entities run the above task but with the S3_KEY updated in the .env file to:

    S3_KEY=entity-builder/dataset/entity.sqlite3

then run:

    docker-compose run --rm task


