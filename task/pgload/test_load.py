import logging
import os
import sys
import pytest
import psycopg2
from pgload.load import do_replace,export_tables

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

def test_connect():
    host = 'localhost'
    database = 'digital_land'
    user = 'postgres'
    password = 'postgres'
    port = 5432
    
    conn = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )
    
    assert conn.status == psycopg2.extensions.STATUS_READY
    conn.close()


@pytest.fixture(scope="module")
def source():
    return "article-4-direction.sqlite3"

def test_csv_files_created(source):
    
    def create_fileName_for_table(table):
        return f"exported_{table}.csv"
    
    # arrange
    os.putenv('DATABASE_NAME',source)
    tables_to_export = ["entity", "old_entity"]
    
    #do_replace(source)
    for table in tables_to_export:
        csv_filename=create_fileName_for_table(table)
        # if os.path.isfile(csv_filename):
        #     os.remove(csv_filename)

    print("tables : ",tables_to_export)
    
    # act do_replace
    do_replace(source,tables_to_export)
    
    # assert
    for table in tables_to_export:
        csv_filename=create_fileName_for_table(table)
        assert os.path.isfile(csv_filename), f"file does not exists:{csv_filename}"
        
        
        
