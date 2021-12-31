#! /usr/bin/env python

import os
import logging
import sys
import csv
import psycopg2
import click

csv.field_size_limit(sys.maxsize)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


@click.command()
@click.option("--csvfile", required=True)
@click.option("--source", required=True)
def do_upsert(csvfile, source):

    host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
    database = os.getenv("DB_NAME", "digital_land")
    user = os.getenv("DB_USER_NAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    logger.info(f"Loading data from {source}")
    
    if source == "entity":
        from pgload.sql import EntitySQL as sql
    elif source == "digital-land":
        from pgload.sql import DatasetSQL as sql
    else:
        logger.info(f"can't import from {source}")
        sys.exit(0)

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(sql.clone_table)
        with open(csvfile) as f:
            cursor.copy_expert(sql.copy, f)
        cursor.execute(sql.upsert)


if __name__ == "__main__":

    logger.info("Loading data into postgres")
    do_upsert()
    logger.info("Finished")
