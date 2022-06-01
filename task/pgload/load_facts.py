import logging
import pathlib
import sys
import subprocess
import tempfile
import os
import click
import psycopg2
import concurrent.futures

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
database = os.getenv("DB_NAME", "digital_land")
user = os.getenv("DB_USER_NAME", "postgres")
password = os.getenv("DB_PASSWORD", "postgres")
port = 5432


@click.command()
@click.option("--bucket", required=True)
def load_facts(bucket):
    logger.info("Load facts running")

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT collection, dataset FROM dataset WHERE collection IS NOT NULL;"
        )
        results = cursor.fetchall()

    collections = {}

    for row in results:
        collection = row[0]
        dataset = row[1]
        if collection in collections:
            collections[collection].append(dataset)
        else:
            collections[collection] = [dataset]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for collection, datasets in collections.items():
            futures.append(
                executor.submit(
                    load_facts_by_collection,
                    bucket=bucket,
                    collection=collection,
                    datasets=datasets,
                )
            )
        for future in concurrent.futures.as_completed(futures):
            print(future.result())


def load_facts_by_collection(bucket, collection, datasets):
    for dataset in datasets:
        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(
                f"loading facts from collection: {collection} dataset: {dataset}"
            )
            csv_path = get_and_export_dataset_sqlite(
                bucket, collection, dataset, tmpdir
            )
            if csv_path is not None:
                try:
                    connection = psycopg2.connect(
                        host=host,
                        database=database,
                        user=user,
                        password=password,
                        port=port,
                    )

                    temp_table_name = f"__temp_table_{dataset.replace('-', '_')}"

                    clone = f"CREATE TEMPORARY TABLE {temp_table_name} AS (SELECT * FROM fact LIMIT 0);"

                    copy = f"""
                        COPY {temp_table_name} (
                            fact, entity, field, value, reference_entity, entry_date, start_date, end_date
                        ) FROM STDIN WITH (
                            FORMAT CSV, 
                            HEADER, 
                            DELIMITER '|', 
                            FORCE_NULL(
                                field, value, reference_entity, entry_date, start_date, end_date
                            )
                        )"""
                    upsert = f"""
                        INSERT INTO fact
                        SELECT *
                        FROM {temp_table_name}
                        ON CONFLICT (fact) DO UPDATE
                        SET field=EXCLUDED.field,
                            value=EXCLUDED.value,
                            reference_entity=EXCLUDED.reference_entity,
                            entry_date=EXCLUDED.entry_date,
                            start_date=EXCLUDED.start_date,
                            end_date=EXCLUDED.end_date;"""

                    with connection.cursor() as cursor:
                        cursor.execute(clone)
                        with open(csv_path) as f:
                            cursor.copy_expert(copy, f)
                        cursor.execute(upsert)
                        connection.commit()
                except Exception as e:
                    msg = f"Error trying to load from {csv_path} for dataset {dataset}"
                    logger.exception(msg)
            else:
                msg = f"Could not load facts for {dataset}"
                logger.info(msg)

    return f"Completed load of facts from collection: {collection} datasets: {datasets}"


def get_and_export_dataset_sqlite(bucket, collection, dataset, download_dir):
    url = f"https://{bucket}.s3.eu-west-2.amazonaws.com/{collection}-collection/dataset/{dataset}.sqlite3"
    sqlite_path = os.path.join(download_dir, f"{dataset}.sqlite3")
    result = subprocess.run(["curl", url, "--output", sqlite_path])
    path = pathlib.Path(sqlite_path)
    if result.returncode == 0 and path.exists() and path.stat().st_size > 0:
        csv_path = os.path.join(download_dir, "exported_facts.csv")
        subprocess.run(
            [
                "sqlite3",
                sqlite_path,
                f".output {csv_path}",
                ".read sql/export_facts.sql",
            ]
        )
        return csv_path
    else:
        logger.info(f"Could not get sqlite file from {url}")
        return None


if __name__ == "__main__":
    load_facts()