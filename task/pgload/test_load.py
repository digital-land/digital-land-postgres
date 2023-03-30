import os
import pytest
import psycopg2
from pgload.load import do_replace
from pgload.sql import SQL

host = 'localhost'
database = 'digital_land'
user = 'postgres'
password = 'postgres'
port = 5432
    
conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)

def test_connect():
    assert conn.status == psycopg2.extensions.STATUS_READY

@pytest.fixture(scope="module")
def source():
    return "article-4-direction"

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

            #make_valid_multipolygon
            cursor.execute("""
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
            AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_MultiPolygon';
            """)
            rowcount = cursor.fetchone()[0]
            assert rowcount == 0

            #make_valid_with_handle_geometry_collection
            cursor.execute("""
            SELECT COUNT(*) FROM entity
            WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)
                AND ST_GeometryType(ST_MakeValid(geometry)) = 'ST_GeometryCollection';
            """)
            rowcount = cursor.fetchone()[0]
            assert rowcount == 0





