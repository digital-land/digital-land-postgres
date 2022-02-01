# Load data into digital-land.info postgres database

## Loading data into postgres in AWS

This repository contains code that is used as a runnable task in ECS. The
entry point [task/load.sh](task/load.sh) expects environment variables
are set for:
    
    S3_BUCKET=some-bucket
    S3_KEY=some-key

These provide a bucket and key to load data from. At the moment the keys are assumed to be sqlite files produced by 
the digital land collection process.

The task is triggered by an S3 put object event tracked by AWS Cloudtrail which extracts event metadata for the S3 
bucket and key name and provides those to this container as enviroment variables. 

The setup of this is in the [Tasks module](https://github.com/digital-land/digital-land-infrastructure/tree/main/terraform/modules/tasks)
of the digital-land-terraform repository.

To see how the values for bucket and key are extracted have a [look here](https://github.com/digital-land/digital-land-infrastructure/blob/main/terraform/modules/tasks/main.tf#L136:L155)

## Running locally to load data into local postgres

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

To load the entity database change the S3_KEY to the correct key for the entity sqlite database (see below).


2. **Create a virtualenv and install requirements**

cd into the task directory and run:

      pip install -r requirements.txt

3. **Run the load script in task directory to load digital-land**

Remember the .env file is already set to load the digital-land db
   
    ./load.sh
   
6. **Run the load script to load entity database**
   
Update the S3_KEY in the .env file to S3_KEY=entity-builder/dataset/entity.sqlite3

    ./load.sh
   
You'll notice that the load script downloads sqlite databases and creates csv files in the directory it runs from. These
files are git and docker ignored, so once done loading you can delete. It's a dumb script so each time you run it 
the files get downloaded/created again.
