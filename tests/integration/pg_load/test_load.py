import csv
import os
import sys
import pytest
import sqlite3

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, parent_dir)
from task.pgload.load import (  # noqa: E402
    make_valid_multipolygon,
    make_valid_with_handle_geometry_collection,
    SQL,
    call_sql_queries,
    export_tables,
    do_replace_table,
    update_entity_subdivided,
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

def test_update_entity_subdivided(postgresql_conn):
    cursor = postgresql_conn.cursor()
    
    test_data = [
        (1, "conservation-area", "MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))"),  # simple multipolygon
        (2, "flood-risk-zone", f'MULTIPOLYGON(({",".join(["(0 0, 0 1, 1 1, 1 0, 0 0)"] * 2001)}))'),  # complex multipolygon
        (3, "test-dataset", "LINESTRING(0 0, 1 1, 2 2)"),  # linestring (not polygon)
    ]
    for entity, dataset, geometry in test_data:
        cursor.execute(
            "INSERT INTO entity (entity, dataset, geometry) VALUES (%s, %s, ST_GeomFromText(%s, 4326))",
            (entity, dataset, geometry),
        )
    postgresql_conn.commit()

    update_entity_subdivided(postgresql_conn)
    cursor.execute("SELECT entity, dataset, GeometryType(geometry_subdivided) FROM entity_subdivided")
    results = cursor.fetchall()

    # Validate
    inserted_entities = {row[0] for row in results}

    assert 1 not in inserted_entities 
    assert 2 in inserted_entities 
    assert 3 not in inserted_entities

    cursor.close()
    
def test_unretired_entities(postgresql_conn):
    source = "certificate-of-immunity"
    table = "old_entity"

    def make_sqlite3_conn(rows):
        test_conn = sqlite3.connect(":memory:")
        test_conn.cursor().execute(
            "CREATE TABLE old_entity ("
            "end_date TEXT, entity INTEGER, entry_date TEXT, notes TEXT, "
            "old_entity TEXT PRIMARY KEY, start_date TEXT, status TEXT)"
        )
        for row in rows:
            test_conn.cursor().execute(
                "INSERT INTO old_entity VALUES (?, ?, ?, ?, ?, ?, ?)", row
            )

        return test_conn

    for filename, sqlite_conn, expected_count in [
        ("exported_old_entity_2.csv", make_sqlite3_conn([]), 1),
        ("exported_old_entity_3.csv", make_sqlite3_conn([]), 0),
        ("exported_old_entity_4.csv", make_sqlite3_conn([]), 0),
        ("exported_old_entity_4.csv", make_sqlite3_conn([]), 0),
        (
            "exported_old_entity_4.csv",
            make_sqlite3_conn([("", "", "", "", "2300000", "", "")]),
            5,
        ),
    ]:
        for file in ["exported_old_entity_1.csv", filename]:
            csv_filename = os.path.join("tests/test_data/", file)

            do_replace_table(table, source, csv_filename, postgresql_conn, sqlite_conn)

        cursor = postgresql_conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM old_entity WHERE old_entity >= 2300000 AND old_entity <= 2300100",
        )
        rowcount = cursor.fetchone()[0]
        cursor.close()
        assert rowcount == expected_count
