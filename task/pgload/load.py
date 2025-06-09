#!/usr/bin/env python
import os
import logging
import sys
import csv
import psycopg2.extensions
import urllib.parse as urlparse
import click
import sqlite3

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
@click.option("--sqlite-db", required=True)
@click.option(
    "--specification-dir", type=click.Path(exists=True), default="specification/"
)
def do_replace_cli(source, sqlite_db, specification_dir):
    specification = Specification(path=specification_dir)
    sqlite_conn = sqlite3.connect(sqlite_db)
    valid_datasets = get_valid_datasets(specification)

    if source == "digital-land" or source in valid_datasets:
        do_replace(source, sqlite_conn)
        if source == "digital-land":
            remove_invalid_datasets(valid_datasets)

    return


def get_pg_connection():
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


def do_replace_table(table, source, csv_filename, postgress_conn, sqlite_conn):
    with open(csv_filename, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        fieldnames = reader.fieldnames

    sql = SQL(table=table, fields=fieldnames, source=source)

    # If we don't get any fieldnames, the file is probably blank. Check the table in the sqlite3.
    if not fieldnames:
        rows = (
            sqlite_conn.cursor().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        )
        if rows == 0:
            if source not in ["digital-land", "entity"]:
                with postgress_conn.cursor() as cursor:
                    cursor.execute(sql.update_tables())
                postgress_conn.commit()

    with postgress_conn.cursor() as cursor:
        call_sql_queries(source, table, csv_filename, fieldnames, sql, cursor)

    postgress_conn.commit()

    logger.info(f"Finished loading from database: {source} table: {table}")

    if source != "entity" and table == "entity":
        make_valid_multipolygon(postgress_conn, source)

        make_valid_with_handle_geometry_collection(postgress_conn, source)

        update_entity_subdivided(postgress_conn)


def do_replace(source, sqlite_conn, tables_to_export=None):
    if tables_to_export is None:
        tables_to_export = export_tables[source]

    for table in tables_to_export:
        logger.info(f"Loading from database: {source} table: {table}")

        csv_filename = f"exported_{table}.csv"

        do_replace_table(table, source, csv_filename, get_pg_connection(), sqlite_conn)


def remove_invalid_datasets(valid_datasets):
    """
    A function that uses the digital_land specification to delete unwanted datasets
    from the postgres database. This keeps the site aligned with the spec
    """
    valid_datasets_str = "', '".join(valid_datasets)
    connection = get_pg_connection()
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

def update_entity_subdivided(connection):
    delete_sql = "DELETE FROM entity_subdivided"
    insert_sql = """
        INSERT INTO entity_subdivided (entity, dataset, geometry_subdivided)
        SELECT e.entity, e.dataset, ST_Multi(g.geom)
        FROM entity e
        JOIN LATERAL (
            SELECT (ST_Dump(ST_Subdivide(ST_MakeValid(e.geometry)))).geom
        ) AS g ON true
        WHERE e.geometry IS NOT NULL
          AND (
              NOT ST_IsValid(e.geometry)
              OR ST_NPoints(e.geometry) > 10000
          )
          AND GeometryType(g.geom) IN ('POLYGON', 'MULTIPOLYGON');
    """

    with connection.cursor() as cursor:
        cursor.execute(delete_sql)
        deleted_count = cursor.rowcount

        cursor.execute(insert_sql)
        rowcount = cursor.rowcount

    connection.commit()
    logger.info(f"Updated entity_subdivided table - {deleted_count} rows deleted")
    logger.info(f"Updated entity_subdivided table - {rowcount} rows with subdivided geometries")


if __name__ == "__main__":
    do_replace_cli()
