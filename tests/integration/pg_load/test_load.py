import csv
import os
import sys
import pytest
import sqlite3
from unittest.mock import patch

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, parent_dir)
from task.pgload.load import (  # noqa: E402
    make_valid_multipolygon,
    make_valid_with_handle_geometry_collection,
    remove_unfixable_invalid_geometries,
    SQL,
    call_sql_queries,
    export_tables,
    do_replace_table,
    remove_invalid_datasets,
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


def test_remove_unfixable_invalid_geometries(postgresql_conn, create_db, tmp_path):
    source = "article-4-direction"
    entity = 9999991
    issue_dir = tmp_path / "issue"

    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute("CREATE TABLE entity (entity INTEGER, dataset TEXT)")
    sqlite_conn.execute("CREATE TABLE fact (fact TEXT, entity INTEGER, field TEXT)")
    sqlite_conn.execute(
        "CREATE TABLE fact_resource (fact TEXT, resource TEXT, entry_number INTEGER)"
    )
    sqlite_conn.execute(
        "CREATE TABLE issue (entity INTEGER, field TEXT, line_number INTEGER)"
    )
    sqlite_conn.execute("INSERT INTO entity VALUES (?, ?)", (entity, source))
    sqlite_conn.execute("INSERT INTO fact VALUES (?, ?, ?)", ("fact-1", entity, "geometry"))
    sqlite_conn.execute(
        "INSERT INTO fact_resource VALUES (?, ?, ?)",
        ("fact-1", "resource-1", 7),
    )
    sqlite_conn.execute("INSERT INTO issue VALUES (?, ?, ?)", (entity, "geometry", 9))

    cursor = postgresql_conn.cursor()
    cursor.execute(
        """
            INSERT INTO entity (
                entity,
                name,
                dataset,
                reference,
                typology,
                geometry
            ) VALUES (
                %s,
                'Unfixable invalid geometry',
                %s,
                'unfixable-invalid-geometry',
                'geography',
                ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 1,0 0))')
            );
        """,
        (entity, source),
    )
    postgresql_conn.commit()
    cursor.close()

    remove_unfixable_invalid_geometries(
        postgresql_conn, source, sqlite_conn=sqlite_conn, issue_dir=str(issue_dir)
    )

    cursor = postgresql_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM entity WHERE entity = %s", (entity,))
    rowcount = cursor.fetchone()[0]
    cursor.close()

    assert rowcount == 0

    issue_path = issue_dir / source / "resource-1.csv"
    with open(issue_path, newline="") as f:
        rows = list(csv.DictReader(f))

    assert rows == [
        {
            "dataset": source,
            "resource": "resource-1",
            "line-number": "9",
            "entry-number": "7",
            "field": "geometry",
            "entity": str(entity),
            "issue-type": "invalid geometry - not fixable",
            "value": "Self-intersection[0.5 0.5]",
            "message": "Geometry remained invalid after PostGIS repair",
        }
    ]


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


def test_remove_invalid_datasets_deletes_from_old_entity(postgresql_conn, create_db):
    cursor = postgresql_conn.cursor()

    # Insert one row with a valid dataset and one with an invalid dataset
    cursor.execute(
        "INSERT INTO old_entity (old_entity, entry_date, start_date, end_date, dataset, notes, status, entity) "
        "VALUES (9990001, '', '', '', 'valid-dataset', '', '', NULL)"
    )
    cursor.execute(
        "INSERT INTO old_entity (old_entity, entry_date, start_date, end_date, dataset, notes, status, entity) "
        "VALUES (9990002, '', '', '', 'invalid-dataset', '', '', NULL)"
    )
    postgresql_conn.commit()
    cursor.close()

    with patch("task.pgload.load.get_pg_connection", return_value=postgresql_conn):
        remove_invalid_datasets(["valid-dataset", "article-4-direction", "ancient-woodland"])

    cursor = postgresql_conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM old_entity WHERE dataset = 'invalid-dataset'")
    assert cursor.fetchone()[0] == 0, "invalid-dataset rows should be removed from old_entity"

    cursor.execute("SELECT COUNT(*) FROM old_entity WHERE old_entity = 9990001")
    assert cursor.fetchone()[0] == 1, "valid-dataset row should remain in old_entity"

    cursor.close()
