import csv
import os
import pytest 
import psycopg2
from task.pgload.load import do_replace,make_valid_multipolygon,make_valid_with_handle_geometry_collection,SQL

host = 'localhost'
database = 'digital_land'
user = 'postgres'
password = 'postgres'
port = 5432
    
conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)

def test_connect():
    assert conn.status == psycopg2.extensions.STATUS_READY

#fixture to set source
@pytest.fixture(scope="module")
def source():
    return "article-4-direction"

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
def test_do_replace(source):
    
    # arrange
    tables_to_export = ["entity", "old_entity"]

    # act do_replace
    do_replace(source,tables_to_export)
    
    # assert
    for table in tables_to_export:
        csv_filename=f"exported_{table}.csv"
        assert os.path.isfile(csv_filename), f"file does not exists:{csv_filename}"
        
        sql_file = SQL('entity', ['col1', 'col2'], 'mydata')

        assert sql_file.update_tables() == f"""
            DELETE FROM entity WHERE dataset = 'mydata';
        """
        assert sql_file.copy_entity() ==f"""
            COPY entity (
                {",".join(['col1', 'col2'])}
            ) FROM STDIN WITH (
                FORMAT CSV,
                HEADER,
                DELIMITER '|',
                FORCE_NULL(
                    {",".join(['col1', 'col2'])}
                )
            );
        """
        with conn.cursor() as cursor:

            multipolygon_check(cursor)
            handle_geometry_collection_check(cursor)

#Initialises PostgreSQL database, creates and inserts data in a new table called entity
@pytest.fixture
def postgresql_connection(postgresql):
    cursor = postgresql.cursor()
    cursor.execute("CREATE EXTENSION postgis")
   
    cursor.execute("CREATE TABLE entity (entity bigint,name varchar, geometry geometry null,point varchar)")
    
    with open('pgload/test_data/entities.testcsv', 'r') as f:
      
        reader = csv.reader(f,delimiter='|')
        next(reader)  
        for row in reader:
            cursor.execute("INSERT INTO entity (entity, name, geometry, point) VALUES (%s, %s, %s, %s)", row)
    postgresql.commit()
    cursor.close()
    return postgresql


def test_make_valid_multipolygon(postgresql_connection):

    cursor = postgresql_connection.cursor()
    make_valid_multipolygon(postgresql_connection)

    multipolygon_check(cursor)

    postgresql_connection.commit()
    cursor.close()
    
def test_make_valid_with_handle_geometry_collection(postgresql_connection):

    cursor = postgresql_connection.cursor()
    make_valid_with_handle_geometry_collection(postgresql_connection)
    
    handle_geometry_collection_check(cursor)

    postgresql_connection.commit()
    cursor.close()