import csv
import os
import sys
import pytest

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, parent_dir)
from task.pgload.load import (  # noqa: E402
    make_valid_multipolygon,
    make_valid_with_handle_geometry_collection,
    SQL,
    call_sql_queries,
    export_tables,
)


# fixture to set source
@pytest.fixture(scope="module")
def sources():
    data = ["article-4-direction", "digital-land", "ancient-woodland"]
    return data


# function to check if invalid data is updated correctly
def multipolygon_check(cursor, source):
    cursor.execute(
        """
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon'
                AND dataset = %s
                AND (
                    (ST_IsSimple(geometry) AND NOT ST_IsValid(geometry))
                    OR NOT ST_IsSimple(geometry));
            """,
        (source,),
    )
    rowcount = cursor.fetchone()[0]
    assert rowcount == 0


# function to check if invalid data is updated correctly
def handle_geometry_collection_check(cursor, source):
    cursor.execute(
        """
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon'
                AND dataset = %s
                AND (
                    (ST_IsSimple(geometry) AND NOT ST_IsValid(geometry))
                    OR NOT ST_IsSimple(geometry));
            """,
        (source,),
    )
    rowcount = cursor.fetchone()[0]
    assert rowcount == 0


# integration test for do_replace function TODO: this should actually use the do replace function
def test_do_replace(sources, postgresql_conn, create_db):
    print("System path::::::", sys.path)
    for source in sources:
        print("Testing do_replace method for source:: ", source)
        if source != "digital-land":
            tables_to_export = ["entity", "old_entity"]
        else:
            tables_to_export = export_tables[source]

        for table in tables_to_export:
            csv_filename = f"exported_{table}.csv"

            with open("tests/test_data/" + csv_filename, "r") as f:
                reader = csv.DictReader(f, delimiter="|")
                fieldnames = reader.fieldnames

            sql = SQL(table=table, fields=fieldnames, source=source)

            with postgresql_conn.cursor() as cursor:
                call_sql_queries(
                    source,
                    table,
                    "tests/test_data/" + csv_filename,
                    fieldnames,
                    sql,
                    cursor,
                )

            postgresql_conn.commit()
        print("Testing do_replace method for source successful:: ", source)


def test_make_valid_multipolygon(postgresql_conn, sources):
    cursor = postgresql_conn.cursor()
    for source in sources:
        make_valid_multipolygon(postgresql_conn, source)
        multipolygon_check(cursor, source)
    postgresql_conn.commit()
    cursor.close()


def test_make_valid_with_handle_geometry_collection(postgresql_conn, sources):
    cursor = postgresql_conn.cursor()
    for source in sources:
        make_valid_with_handle_geometry_collection(postgresql_conn, source)
        handle_geometry_collection_check(cursor, source)
    postgresql_conn.commit()
    cursor.close()


def test_unretired_entities(postgresql_conn):
    source = "certificate-of-immunity"
    table = "old_entity"

    for file in ["exported_old_entity_1.csv", "exported_old_entity_2.csv"]:
        csv_filename = os.path.join("tests/test_unretired/", file)

        with open(csv_filename, "r") as f:
            reader = csv.DictReader(f, delimiter="|")
            fieldnames = reader.fieldnames

        sql = SQL(table=table, fields=fieldnames, source=source)

        with postgresql_conn.cursor() as cursor:
            call_sql_queries(
                source,
                table,
                csv_filename,
                fieldnames,
                sql,
                cursor,
            )

        postgresql_conn.commit()

    cursor = postgresql_conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM old_entity WHERE old_entity >= 2300000 AND old_entity <= 2300100",
    )
    rowcount = cursor.fetchone()[0]
    cursor.close()
    assert rowcount == 1


def test_unretired_entities_blank(postgresql_conn):
    source = "certificate-of-immunity"
    table = "old_entity"

    for file in ["exported_old_entity_1.csv", "exported_old_entity_3.csv"]:
        csv_filename = os.path.join("tests/test_unretired/", file)

        with open(csv_filename, "r") as f:
            reader = csv.DictReader(f, delimiter="|")
            fieldnames = reader.fieldnames

        sql = SQL(table=table, fields=fieldnames, source=source)

        with postgresql_conn.cursor() as cursor:
            call_sql_queries(
                source,
                table,
                csv_filename,
                fieldnames,
                sql,
                cursor,
            )

        postgresql_conn.commit()

    cursor = postgresql_conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM old_entity WHERE old_entity >= 2300000 AND old_entity <= 2300100",
    )
    rowcount = cursor.fetchone()[0]
    cursor.close()
    assert rowcount == 0
