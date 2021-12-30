#! /usr/bin/env python

import logging
import sys
import csv
import psycopg2
import click

csv.field_size_limit(sys.maxsize)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


@click.command()
@click.option("--host", default="localhost")
@click.option("--database", default="digital_land")
@click.option("--user", default="postgres")
@click.option("--password", default="postgres")
@click.option("--csvfile", required=True)
@click.option("--source", required=True)
def do_entity_upsert(host, database, user, password, csvfile, source):

    if source == "entity":
        from pgload.sql import EntitySQL as sql
    elif source == "digital-land":
        logger.info("not implemented")
        sys.exit(0)
    else:
        logger.info(f"can't import from {source}")
        sys.exit(0)

    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(sql.clone_table)
        with open(csvfile) as f:
            cursor.copy_expert(sql.copy, f)
        cursor.execute(sql.upsert)


if __name__ == "__main__":

    logger.info("Loading data into postgres")
    do_entity_upsert()
    logger.info("Finished")
