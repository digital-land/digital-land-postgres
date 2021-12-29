# Manage digital land postgres

**[WIP] Don't expect this to fully, reliably or consistently work. It's early days**

This is a temporary repo to assist in loading data into the digital last postgres db. When 
we have a fully automated process this can be archived.

To use this, create a temporary ec2 instances with ssh access, but limit access from your current IP. 

Use that instance to download scripts in the runner directory and use them to load data.


## How to use this thing locally

Prerequisites

A running postgres server - tested with PostgreSQL 13

Load csv file exported form entity.sqlite3 into a running postgres server on localhost

    docker-compose build

    docker-compose run --rm runner ./fetch_and_load_data.sh
