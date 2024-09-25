#!/usr/bin/env python
import os
import logging
import sys
import csv
import psycopg2.extensions
import urllib.parse as urlparse
import click

# load in specification
from digital_land.specification import Specification


root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(root_dir)
from pgload.sql import SQL  # noqa: E402

csv.field_size_limit(sys.maxsize)

DATABASE_NAME = os.getenv("DATABASE_NAME")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

export_tables = {
    DATABASE_NAME: ["entity", "old_entity"],
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


def get_valid_datasets(specification):
    valid_datasets = [
        dataset["dataset"]
        for dataset in specification.dataset.values()
        if dataset["collection"]
    ]
    return valid_datasets


@click.command()
@click.option("--source", required=True)
@click.option(
    "--specification-dir", type=click.Path(exists=True), default="specification/"
)
def do_replace_cli(source, specification_dir):
    specification = Specification(path=specification_dir)
    valid_datasets = get_valid_datasets(specification)

    if source == "digital-land" or source in valid_datasets:
        do_replace(source)
        if source == "digital-land":
            remove_invalid_datasets(valid_datasets)

    return


def get_connection():
    try:
        url = urlparse.urlparse(os.getenv("WRITE_DATABASE_URL"))
        database = url.path[1:]
        user = url.username
        password = url.password
        host = url.hostname
        port = url.port
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
    except:  # noqa: E722
        host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
        database = os.getenv("DB_NAME", "digital_land")
        user = os.getenv("DB_USER_NAME", "postgres")
        password = os.getenv("DB_PASSWORD", "postgres")
        port = 5432
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )

    connection.autocommit = False

    return connection


def do_replace(source, tables_to_export=None):
    if tables_to_export is None:
        tables_to_export = export_tables[source]

    connection = get_connection()

    for table in tables_to_export:
        logger.info(f"Loading from database: {source} table: {table}")

        csv_filename = f"exported_{table}.csv"

        with open(csv_filename, "r") as f:
            reader = csv.DictReader(f, delimiter="|")
            fieldnames = reader.fieldnames

        sql = SQL(table=table, fields=fieldnames, source=source)

        with connection.cursor() as cursor:
            call_sql_queries(source, table, csv_filename, fieldnames, sql, cursor)

        connection.commit()

        logger.info(f"Finished loading from database: {source} table: {table}")

        if source != "entity" and table == "entity":
            make_valid_multipolygon(connection, source)

            make_valid_with_handle_geometry_collection(connection, source)

            update_simplified_geometry_column(connection,source)


def remove_invalid_datasets(valid_datasets):
    """
    A function that uses the digital_land specification to delete unwanted datasets
    from the postgres database. This keeps the site aligned with the spec
    """
    valid_datasets_str = "', '".join(valid_datasets)
    connection = get_connection()
    # remove datasets not in valid_datasets from entity
    with connection.cursor() as cursor:
        sql = f"""
            DELETE FROM entity WHERE dataset not in ('{valid_datasets_str}');
        """
        cursor.execute(sql)

    connection.commit()

    # TODO remove old_entities as well but given how the ranges work this isn't important for now


def call_sql_queries(source, table, csv_filename, fieldnames, sql, cursor):
    if fieldnames is not None:
        if source == "digital-land":
            cursor.execute(sql.clone_table())
            with open(csv_filename) as f:
                cursor.copy_expert(sql.copy(), f)
            cursor.execute(sql.rename_tables())
            cursor.execute(sql.drop_clone_table())
        elif source != "entity":
            cursor.execute("select count(*) from " + table)
            cursor.execute(sql.update_tables())
            cursor.execute("select count(*) from entity")
            with open(csv_filename) as f:
                cursor.copy_expert(sql.copy_entity(), f)

            cursor.execute("select count(*) from entity")
    else:
        logger.info(f"No data found in database: {source} table: {table}")


def make_valid_with_handle_geometry_collection(connection, source):
    make_valid_with_handle_geometry_collection = """
                UPDATE entity SET geometry = ST_CollectionExtract(ST_MakeValid(geometry))
                WHERE geometry IS NOT NULL
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_GeometryCollection' AND  dataset = %s
                AND (
                    (ST_IsSimple(geometry) AND NOT ST_IsValid(geometry))
                    OR NOT ST_IsSimple(geometry));
                """.strip()

    with connection.cursor() as cursor:
        cursor.execute(make_valid_with_handle_geometry_collection, (source,))
        rowcount = cursor.rowcount
        connection.commit()

    logger.info(
        f"Updated {rowcount} rows with valid geometry collections converted to multi polygons"
    )


def make_valid_multipolygon(connection, source):
    make_valid_multipolygon = """
                UPDATE entity
            SET geometry = ST_MakeValid(geometry)
            WHERE geometry IS NOT NULL
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon'
                AND dataset = %s
                AND (
                    (ST_IsSimple(geometry) AND NOT ST_IsValid(geometry))
                    OR NOT ST_IsSimple(geometry));
        """.strip()

    with connection.cursor() as cursor:
        cursor.execute(make_valid_multipolygon, (source,))
        rowcount = cursor.rowcount
        connection.commit()

    logger.info(f"Updated {rowcount} rows with valid multi polygons")

def update_simplified_geometry_column(connection,source):
    simplified_geometry_column_update = """
                UPDATE entity
            SET simplified_geometry = ST_SimplifyPreserveTopology(geometry, 0.0001)
            WHERE dataset = %s and
            geometry IS NOT NULL AND ST_GeometryType(geometry) = 'ST_MultiPolygon';
        """.strip()
    

    with connection.cursor() as cursor:
        cursor.execute(simplified_geometry_column_update, (source,))
        rowcount = cursor.rowcount
        connection.commit()

    logger.info(f"Updated {rowcount} rows with simplified multi polygons")

if __name__ == "__main__":
    do_replace_cli()
