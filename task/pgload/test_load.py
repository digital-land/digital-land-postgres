import csv
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

def test_postgres(postgresql):

    cur = postgresql.cursor()
    cur.execute("CREATE TABLE entity (entity bigint,name varchar,entry_date varchar,start_date varchar,end_date varchar,dataset varchar,json varchar,organisation_entity varchar,prefix varchar,reference varchar,typology varchar,geometry varchar,point varchar)")
    
    with open('entities.csv', 'r') as f:
      
        reader = csv.reader(f,delimiter='|')
        next(reader)  
        for row in reader:
            cur.execute("INSERT INTO entity (entity, name, entry_date, start_date, end_date, dataset, json, organisation_entity, prefix, reference, typology, geometry, point) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

    postgresql.commit()
    cur.execute("SELECT * FROM entity limit 11")
    result = cur.fetchone()
    print(result)  
    assert len(result)<0
    cur.close()

    #cur.execute("INSERT INTO entity (id, name) VALUES (1, 'John')")
    # assert result == (1, 'John')
    # postgresql.commit()
    

# postgresql = factories.postgresql()

# # @pytest.fixture(scope='session')
# # def postgresql_database(postgresql):
# #     return postgresql

# def test_insert_data(postgresql):
#     conn = psycopg2.connect(**postgresql_database.dsn())
#     cur = conn.cursor()
#     cur.execute("INSERT INTO entity (id, name) VALUES (1, 'John')")
#     conn.commit()
#     cur.execute("SELECT * FROM entity")
#     result = cur.fetchone()
#     assert result == (1, 'John')
#     cur.close()
#     conn.close()

