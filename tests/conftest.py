import pytest
import os
import csv
import psycopg2.extensions


@pytest.fixture(scope="session")
def postgresql_conn():
    host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
    database = os.getenv("DB_NAME", "digital_land_test")
    user = os.getenv("DB_USER_NAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    port = 5432
    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )

    yield connection
    connection.close()


@pytest.fixture(scope="session")
def create_db(postgresql_conn):
    # Create a connection to the PostgreSQL server
    cursor = postgresql_conn.cursor()
    # Create the table and load the data
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    cursor.execute(
        """
        CREATE TABLE entity (
            entity bigint,
            name varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            dataset varchar,
            json varchar,
            organisation_entity varchar,
            prefix varchar,
            reference varchar,
            typology varchar,
            geojson varchar,
            geometry geometry null,
            point varchar
        )"""
    )
    cursor.execute(
        """
        CREATE TABLE old_entity (
            old_entity bigint,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            dataset varchar,
            notes varchar,
            status varchar,
            entity varchar null
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset (
        dataset varchar,
        name varchar,
        entry_date varchar,
        start_date varchar,
        end_date varchar,
        collection varchar,
        description varchar,
        key_field varchar,
        paint_options varchar,
        plural varchar,
        prefix varchar,
        text varchar,
        typology varchar,
        wikidata varchar,
        wikipedia varchar,
        themes varchar,
        attribution_id varchar,
        licence_id varchar,
        consideration varchar,
        github_discussion varchar,
        entity_minimum varchar,
        entity_maximum varchar,
        phase varchar,
        realm varchar,
        version varchar,
        replacement_dataset varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE typology (
            typology varchar,
            name varchar,
            description varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            plural varchar,
            text varchar,
            wikidata varchar,
            wikipedia varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE organisation (
        organisation varchar,
        name varchar,
        combined_authority varchar,
        entry_date varchar,
        start_date varchar,
        end_date varchar,
        entity varchar,
        local_authority_type varchar,
        official_name varchar,
        region varchar,
        statistical_geography varchar,
        website varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset_collection (
            dataset_collection varchar,
            resource varchar,
            resource_end_date varchar,
            resource_entry_date varchar,
            last_updated varchar,
            last_collection_attempt varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset_publication (
            dataset_publication varchar,
            expected_publisher_count varchar,
            publisher_count varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE lookup (
            id varchar,entity varchar,
            prefix varchar,
            reference varchar,
            entry_date varchar,
            start_date varchar,
            value varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE attribution (
            attribution varchar,
            text varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE licence (
            licence varchar,
            text varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE entity_subdivided (
            entity_subdivided_id BIGSERIAL PRIMARY KEY,
            entity bigint,
            dataset text,
            geometry_subdivided geometry
        )"""
    )

    with open("tests/test_data/exported_entity.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO entity (
                    entity,
                    name,
                    entry_date,
                    start_date,
                    end_date,
                    dataset,
                    json,
                    organisation_entity,
                    prefix,
                    reference,
                    typology,
                    geojson,
                    geometry,
                    point
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_old_entity.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO old_entity (
                    entity,
                    old_entity,
                    entry_date,
                    start_date,
                    end_date,
                    status,
                    notes,
                    dataset
                ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_dataset.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO dataset (
                    dataset,
                    name,
                    entry_date,
                    start_date,
                    end_date,
                    collection,
                    description,
                    key_field,
                    paint_options,
                    plural,prefix,
                    text,typology,
                    wikidata,
                    wikipedia,
                    themes,
                    attribution_id,
                    licence_id,
                    consideration,
                    github_discussion,
                    entity_minimum,
                    entity_maximum,
                    phase,
                    realm,
                    version,
                    replacement_dataset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_typology.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO typology (
                    typology,
                    name,
                    description,
                    entry_date,
                    start_date,
                    end_date,
                    plural,
                    text,
                    wikidata,
                    wikipedia
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_organisation.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO organisation (
                    organisation,
                    name,
                    combined_authority,
                    entry_date,
                    start_date,
                    end_date,
                    entity,
                    local_authority_type,
                    official_name,
                    region,
                    statistical_geography,
                    website
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_dataset_collection.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO dataset_collection (
                    dataset_collection,
                    resource,
                    resource_end_date,
                    resource_entry_date,
                    last_updated,
                    last_collection_attempt
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_dataset_publication.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO dataset_publication (
                    dataset_publication,expected_publisher_count,publisher_count
                ) VALUES (%s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_lookup.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO lookup (
                    id,entity,prefix,reference,entry_date,start_date,value
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_attribution.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO attribution (
                    attribution,text,entry_date,start_date,end_date
                ) VALUES (%s, %s, %s, %s, %s)
            """,
                row,
            )

    with open("tests/test_data/exported_licence.csv", "r") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO licence (
                    licence,text,entry_date,start_date,end_date
                ) VALUES (%s, %s, %s, %s, %s)
            """,
                row,
            )

    postgresql_conn.commit()
    cursor.close()


@pytest.fixture(scope="session", autouse=True)
def finalizer(request, postgresql_conn):
    def close_connection():
        print("All tests running finished!!!!!")
        cursor = postgresql_conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS entity;")
        cursor.execute("DROP TABLE IF EXISTS old_entity;")
        cursor.execute("DROP TABLE IF EXISTS dataset;")
        cursor.execute("DROP TABLE IF EXISTS typology;")
        cursor.execute("DROP TABLE IF EXISTS organisation;")
        cursor.execute("DROP TABLE IF EXISTS dataset_collection;")
        cursor.execute("DROP TABLE IF EXISTS dataset_publication;")
        cursor.execute("DROP TABLE IF EXISTS lookup;")
        cursor.execute("DROP TABLE IF EXISTS attribution;")
        cursor.execute("DROP TABLE IF EXISTS licence;")
        cursor.execute("DROP TABLE IF EXISTS entity_subdivided;")
        postgresql_conn.commit()
        cursor.close()
        postgresql_conn.close()

    request.addfinalizer(close_connection)
