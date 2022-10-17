import logging
import pathlib
import sys
import subprocess
import tempfile
import os
import click
import psycopg2
import concurrent.futures
import urllib.parse as urlparse

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

try:
    url = urlparse.urlparse(os.getenv('WRITE_DATABASE_URL'))
    database = url.path[1:]
    user = url.username
    password = url.password
    host = url.hostname
    port = url.port
except:
    host = os.getenv("DB_WRITE_ENDPOINT", "localhost")
    database = os.getenv("DB_NAME", "digital_land")
    user = os.getenv("DB_USER_NAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    port = 5432


datasette_url = "https://datasette.digital-land.info/{dataset}/fact.json?_shape=objects&_labels=off&_size=max"


@click.command()
def load_facts():
    logger.info("Load facts")

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
                    load_facts_by_collection, collection=collection, datasets=datasets
                )
            )
        for future in concurrent.futures.as_completed(futures):
            logger.info(future.result())


def load_facts_into_postgres(rows):

    for row in rows:
        for key, val in row.items():
            if not val:
                row[key] = None

    raw_keys = rows[0].keys()
    columns = ",".join(raw_keys)
    values = ",".join([f"%({key})s" for key in raw_keys])
    conflicting = ",".join([f"{key}=EXCLUDED.{key}" for key in raw_keys])
    on_conflict = f"SET {conflicting}"

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )

    with connection.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO fact ({columns}) VALUES ({values}) ON CONFLICT (fact) DO UPDATE {on_conflict};",
            rows,
        )

    connection.commit()
    return len(rows)


def load_facts_by_collection(collection, datasets):

    for dataset in datasets:
        logger.info(f"loading facts from collection: {collection} dataset: {dataset}")
        url = datasette_url.format(dataset=dataset)
        total_inserted = 0
        while url:
            resp = requests.get(url)
            data = resp.json()
            if data.get("rows"):
                total_inserted += load_facts_into_postgres(data["rows"])
                total_records = data["filtered_table_rows_count"]
                logger.info(
                    f"inserted {total_inserted} of {total_records} from {dataset}"
                )
            url = data.get("next_url", False)
        logger.info(f"Done fetching {dataset}")
    return f"Done loading collection: {collection} dataset: {dataset}"


if __name__ == "__main__":
    load_facts()
