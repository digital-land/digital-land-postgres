#!/usr/bin/env python
"""
Removes a retired dataset's rows from Postgres. Run by the retire-dataset DAG in airflow-dags once the
dataset's environment value in the specification no longer includes this environment.
"""
import os
import sys
import logging
import click

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(root_dir)
from pgload.load import get_pg_connection  # noqa: E402

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# The tables a dataset's own load writes to: entity and old_entity in do_replace, entity_subdivided
# in update_entity_subdivided
RETIRE_TABLES = ["entity", "old_entity", "entity_subdivided"]


def is_dry_run(dry_run):
    """Only an explicit "false" removes rows, so a missing or mistyped value is a dry run."""
    return dry_run.strip().lower() != "false"


def retire_dataset_rows(connection, dataset, dry_run=True):
    """
    Count the dataset's rows in each table and, unless this is a dry run, delete them. Everything runs in
    one transaction, so either every table loses the dataset's rows or none do.
    """
    counts = {}
    try:
        with connection.cursor() as cursor:
            for table in RETIRE_TABLES:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE dataset = %s;", (dataset,)
                )
                counts[table] = cursor.fetchone()[0]
                if not dry_run:
                    cursor.execute(
                        f"DELETE FROM {table} WHERE dataset = %s;", (dataset,)
                    )
    except Exception:
        connection.rollback()
        raise

    if dry_run:
        connection.rollback()
    else:
        connection.commit()

    return counts


@click.command()
@click.option("--dataset", required=True)
@click.option(
    "--dry-run", default="true", help='Rows are only removed when this is "false"'
)
def retire_dataset_cli(dataset, dry_run):
    dry_run = is_dry_run(dry_run)
    connection = get_pg_connection()
    try:
        counts = retire_dataset_rows(connection, dataset, dry_run)
    finally:
        connection.close()

    action = "would remove" if dry_run else "removed"
    for table, count in counts.items():
        logger.info(f"{action} {count} {dataset} rows from {table}")


if __name__ == "__main__":
    retire_dataset_cli()
