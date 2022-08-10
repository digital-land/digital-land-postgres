#!/usr/bin/env python

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

export_tables = {
    "entity": ["entity", "old_entity"],
    "digital-land": [
        "dataset",
        "typology",
        "organisation",
        "dataset_collection",
        "dataset_publication",
        "lookup",
        "attribution",
        "licence",
    ],
}


@click.command()
@click.option("--source", required=True)
def do_replace_cli(source):

    host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
    database = os.getenv("DB_NAME", "digital_land")
    user = os.getenv("DB_USER_NAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    port = 5432
    return do_replace(source, host, database, user, password, port)


def do_replace(source, host, database, user, password, port):
    tables_to_export = export_tables[source]

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )
    connection.autocommit = False

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
            cursor.execute(sql.rename_tables())
            cursor.execute(sql.drop_clone_table())
        connection.commit()

        logger.info(f"Finished loading from database: {source} table: {table}")

        if source == "entity" and table == "entity":

            make_valid_multipolygon = """
                UPDATE entity set geometry = ST_MakeValid(geometry)
                WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon';
                """.strip()

            with connection.cursor() as cursor:
                cursor.execute(make_valid_multipolygon)
                rowcount = cursor.rowcount
                connection.commit()

            logger.info(f"Updated {rowcount} rows with valid multi polygons")

            make_valid_with_handle_geometry_collection = """
                UPDATE entity SET geometry = ST_CollectionExtract(ST_MakeValid(geometry))
                WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_GeometryCollection';
                """.strip()

            with connection.cursor() as cursor:
                cursor.execute(make_valid_with_handle_geometry_collection)
                rowcount = cursor.rowcount
                connection.commit()

            logger.info(
                f"Updated {rowcount} rows with valid geometry collections converted to multi polygons"
            )


if __name__ == "__main__":
    do_replace_cli()
