import csv
import os
import pytest 
import psycopg2
from task.pgload.load import SQL,call_sql_queries,export_tables,make_valid_multipolygon,make_valid_with_handle_geometry_collection

#fixture to set source
@pytest.fixture(scope="module")
def sources():
    data = ["article-4-direction"]
    return data

@pytest.fixture
def postgresql_connection_doreplace(postgresql):
    cursor = postgresql.cursor()
    cursor.execute("CREATE EXTENSION postgis")
    cursor.execute("CREATE TABLE entity (entity bigint,name varchar,entry_date varchar,start_date varchar,end_date varchar,dataset varchar,json varchar,organisation_entity varchar,prefix varchar,reference varchar,typology varchar,geojson varchar, geometry geometry null,point varchar)")
    with open('tests/test_data/exported_entity.testcsv', 'r') as f:
      
        reader = csv.reader(f,delimiter='|')
        next(reader)  
        for row in reader:
            cursor.execute("INSERT INTO entity (entity, name,entry_date,start_date,end_date,dataset,json,organisation_entity,prefix,reference,typology,geojson, geometry, point) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)", row)
    cursor.execute("""SELECT COUNT(*) from entity;""") 
    
    row_count = cursor.fetchone()[0]
    print("row count in entity table",row_count)
    #assert False
    postgresql.commit()
    cursor.close()
    return postgresql

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
def test_do_replace(sources,postgresql_connection_doreplace):

    for source in sources:
        if source!='digital-land':
            tables_to_export=["entity", "old_entity"]
        else:
            tables_to_export = export_tables[source]

        for table in tables_to_export:
        
            csv_filename = f"exported_{table}.testcsv"

            with open("tests/test_data/"+csv_filename, "r") as f:
                reader = csv.DictReader(f, delimiter="|")
                fieldnames = reader.fieldnames

            sql = SQL(table=table, fields=fieldnames, source=source)

            with postgresql_connection_doreplace.cursor() as cursor:
                call_sql_queries(source, table, "tests/test_data/"+csv_filename, fieldnames, sql, cursor)

            postgresql_connection_doreplace.commit()

#Initialises PostgreSQL database, creates and inserts data in a new table called entity

@pytest.fixture
def postgresql_connection(postgresql):
    cursor = postgresql.cursor()
    cursor.execute("CREATE EXTENSION postgis")
   
    cursor.execute("CREATE TABLE entity (entity bigint,name varchar, geometry geometry null,point varchar)")
    
    with open('tests/test_data/entities.testcsv', 'r') as f:
      
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