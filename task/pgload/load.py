#!/usr/bin/env python
import os
import logging
import sys
import csv
import psycopg2.extensions
import urllib.parse as urlparse
import click
import sqlite3
from psycopg2.extras import execute_values

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

complex_geom_datasets = ['flood-risk-zone']
export_tables = {
    DATABASE_NAME: ["entity", "old_entity"],
    "digital-land": [
        "dataset",
        "typology",
        "organisation",
        "dataset_collection",
        "dataset_publication",
        # "lookup", TODO add this back in after we update the lookup table
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
@click.option("--issue-dir", default="issue")
def do_replace_cli(source, sqlite_db, specification_dir, issue_dir):
    specification = Specification(path=specification_dir)
    sqlite_conn = sqlite3.connect(sqlite_db)
    valid_datasets = get_valid_datasets(specification)

    if source == "digital-land" or source in valid_datasets:
        do_replace(source, sqlite_conn, issue_dir=issue_dir)
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


def do_replace_table(
    table, source, csv_filename, postgress_conn, sqlite_conn, issue_dir="issue"
):
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

        remove_unfixable_invalid_geometries(
            postgress_conn, source, sqlite_conn=sqlite_conn, issue_dir=issue_dir
        )

        if source in complex_geom_datasets:
            update_entity_subdivided(postgress_conn,source)


def do_replace(source, sqlite_conn, tables_to_export=None, issue_dir="issue"):
    if tables_to_export is None:
        tables_to_export = export_tables[source]

    for table in tables_to_export:
        logger.info(f"Loading from database: {source} table: {table}")

        csv_filename = f"exported_{table}.csv"

        do_replace_table(
            table, source, csv_filename, get_pg_connection(), sqlite_conn, issue_dir
        )


def remove_invalid_datasets(valid_datasets):
    """
    A function that uses the digital_land specification to delete unwanted datasets
    from the postgres database. This keeps the site aligned with the spec
    """
    valid_datasets_str = "', '".join(valid_datasets)
    connection = get_pg_connection()
    # remove datasets not in valid_datasets from entity
    with connection.cursor() as cursor:
        # remove rows from entity that are part of an invalid dataset
        sql = f"""
            DELETE FROM entity WHERE dataset not in ('{valid_datasets_str}');
        """
        cursor.execute(sql)

        # now remove ant rows from old_entity that belong to an invalid dataset
        sql = f"""
            DELETE FROM old_entity WHERE dataset not in ('{valid_datasets_str}');
        """
        cursor.execute(sql)

    connection.commit()


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


ISSUE_FIELDNAMES = [
    "dataset",
    "resource",
    "line-number",
    "entry-number",
    "field",
    "entity",
    "issue-type",
    "value",
    "message",
]


def sqlite_table_exists(sqlite_conn, table):
    cursor = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    )
    return cursor.fetchone() is not None


def sqlite_table_columns(sqlite_conn, table):
    return [row[1] for row in sqlite_conn.execute(f"PRAGMA table_info({table})")]


def entity_load_context_rows(sqlite_conn, source):
    if not sqlite_conn:
        return []

    required_tables = ["entity", "fact", "fact_resource"]
    if not all(sqlite_table_exists(sqlite_conn, table) for table in required_tables):
        return []

    issue_join = ""
    issue_selects = "'' AS line_number"
    if sqlite_table_exists(sqlite_conn, "issue"):
        issue_columns = sqlite_table_columns(sqlite_conn, "issue")
        if "entity" in issue_columns and "line_number" in issue_columns:
            issue_join = """
                LEFT JOIN (
                    SELECT entity, field, MIN(line_number) AS line_number
                    FROM issue
                    WHERE field = 'geometry'
                    GROUP BY entity, field
                ) i
                    ON i.entity = e.entity
                    AND i.field = 'geometry'
            """
            issue_selects = "COALESCE(i.line_number, '') AS line_number"

    rows = sqlite_conn.execute(
        f"""
            SELECT DISTINCT
                e.entity,
                e.dataset,
                COALESCE(fr.resource, '') AS resource,
                {issue_selects},
                COALESCE(fr.entry_number, '') AS entry_number
            FROM entity e
            LEFT JOIN fact f
                ON f.entity = e.entity
                AND f.field = 'geometry'
            LEFT JOIN fact_resource fr
                ON fr.fact = f.fact
            {issue_join}
            WHERE e.dataset = ?;
        """,
        (source,),
    ).fetchall()

    return rows


def load_entity_context(connection, source, sqlite_conn):
    create_context_table = """
        CREATE TEMP TABLE IF NOT EXISTS entity_load_context (
            entity bigint,
            dataset text,
            resource text,
            line_number text,
            entry_number text
        ) ON COMMIT PRESERVE ROWS;
    """.strip()

    delete_context = "DELETE FROM entity_load_context WHERE dataset = %s;"

    rows = entity_load_context_rows(sqlite_conn, source)

    with connection.cursor() as cursor:
        cursor.execute(create_context_table)
        cursor.execute(delete_context, (source,))
        if rows:
            execute_values(
                cursor,
                """
                    INSERT INTO entity_load_context (
                        entity,
                        dataset,
                        resource,
                        line_number,
                        entry_number
                    ) VALUES %s
                """,
                rows,
            )

    connection.commit()


def write_invalid_geometry_issues(issue_dir, source, invalid_geometries):
    if not invalid_geometries:
        return

    for row in invalid_geometries:
        resource = row.get("resource") or "postgres-loader"
        row["resource"] = resource

        path = os.path.join(issue_dir, source, resource + ".csv")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        file_exists = os.path.exists(path)

        with open(path, "a", newline="") as f:
            writer = csv.DictWriter(f, ISSUE_FIELDNAMES)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


def remove_unfixable_invalid_geometries(
    connection, source, sqlite_conn=None, issue_dir="issue"
):
    load_entity_context(connection, source, sqlite_conn)

    select_invalid_geometries = """
        SELECT
            e.dataset,
            COALESCE(c.resource, '') AS resource,
            COALESCE(c.line_number, '') AS line_number,
            COALESCE(c.entry_number, '') AS entry_number,
            'geometry' AS field,
            e.entity,
            'invalid geometry - not fixable' AS issue_type,
            ST_IsValidReason(e.geometry) AS value,
            'Geometry remained invalid after PostGIS repair' AS message
        FROM entity e
        LEFT JOIN entity_load_context c
            ON c.entity = e.entity
            AND c.dataset = e.dataset
        WHERE e.geometry IS NOT NULL
            AND e.dataset = %s
            AND NOT ST_IsValid(e.geometry);
    """.strip()

    delete_invalid_geometries = """
        DELETE FROM entity
        WHERE geometry IS NOT NULL
            AND dataset = %s
            AND NOT ST_IsValid(geometry);
    """.strip()

    drop_context_table = "DROP TABLE IF EXISTS entity_load_context;"

    with connection.cursor() as cursor:
        cursor.execute(select_invalid_geometries, (source,))
        invalid_geometries = [
            {
                "dataset": row[0],
                "resource": row[1],
                "line-number": row[2],
                "entry-number": row[3],
                "field": row[4],
                "entity": row[5],
                "issue-type": row[6],
                "value": row[7],
                "message": row[8],
            }
            for row in cursor.fetchall()
        ]

        write_invalid_geometry_issues(issue_dir, source, invalid_geometries)

        cursor.execute(delete_invalid_geometries, (source,))
        rowcount = cursor.rowcount
        cursor.execute(drop_context_table)
        connection.commit()

    logger.info(f"Removed {rowcount} rows with unfixable invalid geometries")

def update_entity_subdivided(connection, source):   
    delete_sql = """
            DELETE FROM entity_subdivided WHERE dataset = %s ;
        """
    
    update_entity_subdivided = """
        INSERT INTO entity_subdivided (entity, dataset, geometry_subdivided)
            SELECT e.entity, e.dataset, ST_Multi(g.geom)
            FROM entity e
            JOIN LATERAL (
            SELECT (ST_Dump(ST_Subdivide(ST_MakeValid(e.geometry)))).geom
            ) AS g ON true
            WHERE dataset = %s 
            AND e.geometry IS NOT NULL
            AND ST_IsValid(e.geometry);

        """.strip()

    with connection.cursor() as cursor:
        
        cursor.execute(delete_sql, (source,))
        deleted_count = cursor.rowcount

        cursor.execute(update_entity_subdivided, (source,))
        rowcount = cursor.rowcount

    connection.commit()

    logger.info(f"Updated entity_sub_divided table - {deleted_count} rows deleted")
    logger.info(f"Updated entity_sub_divided table - {rowcount} rows with subdivided geometries")

if __name__ == "__main__":
    do_replace_cli()
