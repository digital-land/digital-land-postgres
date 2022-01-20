#! /usr/bin/env python

import os
import logging
import sys
import csv
import psycopg2
import click

from pgload.sql import SQL

csv.field_size_limit(sys.maxsize)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

export_tables = {"entity": ["entity"], "digital-land": ["dataset", "typology", "organisation"]}

@click.command()
@click.option("--source", required=True)
def do_upsert(source):

    host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
    database = os.getenv("DB_NAME", "digital_land")
    user = os.getenv("DB_USER_NAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    tables_to_export = export_tables[source]

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password
    )
    connection.autocommit = True

    for table in tables_to_export:
        logger.info(f"Loading from database: {source} table: {table}")

        csv_filename = f"exported_{table}.csv"
        with open(csv_filename, "r") as f:
            reader = csv.DictReader(f, delimiter="|")
            fieldnames = reader.fieldnames

        sql = SQL(table=table, fields=fieldnames)

        with connection.cursor() as cursor:
            cursor.execute(sql.clone_table())
            with open(csv_filename) as f:
                cursor.copy_expert(sql.copy(), f)
            cursor.execute(sql.upsert())
            cursor.execute(sql.drop_clone_table())

        logger.info(f"Finished loading from database: {source} table: {table}")


if __name__ == "__main__":
    do_upsert()
