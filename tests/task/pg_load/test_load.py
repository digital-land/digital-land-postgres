import csv
import os
import sys
import pytest 
import psycopg2
from conftest import postgresql_conn
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
sys.path.insert(0, parent_dir)
from task.pgload.load import do_replace,make_valid_multipolygon,make_valid_with_handle_geometry_collection,SQL,call_sql_queries,export_tables


   
#fixture to set source
@pytest.fixture(scope="module")
def sources():
    data = ["article-4-direction", "digital-land", "ancient-woodland"]
    return data


#function to check if invalid data is updated correctly
def multipolygon_check(cursor):
    cursor.execute("""
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
            AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon';
            """)
    rowcount = cursor.fetchone()[0]
    assert rowcount == 0

#function to check if invalid data is updated correctly
def handle_geometry_collection_check(cursor):
    cursor.execute("""
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
            AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_GeometryCollection';
            """)
    rowcount = cursor.fetchone()[0]
    assert rowcount == 0

#integration test for do_replace function
def test_do_replace(sources,postgresql_conn, create_db):
    print("System path::::::",sys.path)
    for source in sources:
        print("Testing do_replace method for source:: ", source)
        if source!='digital-land':
            tables_to_export=["entity", "old_entity"]
        else:
            tables_to_export = export_tables[source]

        for table in tables_to_export:
        
            csv_filename = f"exported_{table}.csv"

            with open("tests/test_data/"+csv_filename, "r") as f:
                reader = csv.DictReader(f, delimiter="|")
                fieldnames = reader.fieldnames

            sql = SQL(table=table, fields=fieldnames, source=source)

            with postgresql_conn.cursor() as cursor:
                call_sql_queries(source, table, "tests/test_data/"+csv_filename, fieldnames, sql, cursor)

            postgresql_conn.commit()
        print("Testing do_replace method for source successful:: ", source)


def test_make_valid_multipolygon(postgresql_conn):

    cursor = postgresql_conn.cursor()
    make_valid_multipolygon(postgresql_conn)

    multipolygon_check(cursor)

    postgresql_conn.commit()
    cursor.close()
    
def test_make_valid_with_handle_geometry_collection(postgresql_conn):

    cursor = postgresql_conn.cursor()
    make_valid_with_handle_geometry_collection(postgresql_conn)
    
    handle_geometry_collection_check(cursor)

    postgresql_conn.commit()
    cursor.close()