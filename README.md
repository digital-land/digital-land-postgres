# Manage digital land postgres

**[WIP] Don't expect this to fully, reliably or consistently work. It's early days**

This is a possibly temporary repo to assist in loading data into the digital last postgres db.

## How to use this thing locally

Prerequisites

A running postgres server - tested with PostgreSQL 13

Assumption is target digital_land db already has entity and dataset tables. In other words
migrations up to date.

Load csv file exported form entity.sqlite3 into a running postgres server on localhost

    docker-compose build

then

    docker-compose run --rm task ./load.sh entity 

and then

    docker-compose run --rm task ./load.sh digital-land 


