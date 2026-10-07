import os
import sys
import pytest

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, parent_dir)
from task.pgload.retire import is_dry_run, retire_dataset_rows  # noqa: E402

RETIRED = "retired-dataset"
OTHER = "other-dataset"


def count_rows(postgresql_conn, table, dataset):
    with postgresql_conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE dataset = %s;", (dataset,))
        return cursor.fetchone()[0]


@pytest.fixture
def dataset_rows(postgresql_conn, create_db):
    """Two rows for the retired dataset and one for another dataset in each table."""
    with postgresql_conn.cursor() as cursor:
        # create_db doesn't make entity_subdivided as no other test uses it
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS entity_subdivided "
            "(entity bigint, dataset varchar, geometry_subdivided geometry null);"
        )
        for entity, dataset in [
            (9980001, RETIRED),
            (9980002, RETIRED),
            (9980003, OTHER),
        ]:
            cursor.execute(
                "INSERT INTO entity (entity, dataset) VALUES (%s, %s);",
                (entity, dataset),
            )
            cursor.execute(
                "INSERT INTO old_entity (old_entity, dataset, status) VALUES (%s, %s, '410');",
                (entity, dataset),
            )
            cursor.execute(
                "INSERT INTO entity_subdivided (entity, dataset) VALUES (%s, %s);",
                (entity, dataset),
            )
    postgresql_conn.commit()

    yield

    with postgresql_conn.cursor() as cursor:
        for table in ["entity", "old_entity", "entity_subdivided"]:
            cursor.execute(
                f"DELETE FROM {table} WHERE dataset IN (%s, %s);", (RETIRED, OTHER)
            )
        cursor.execute("DROP TABLE entity_subdivided;")
    postgresql_conn.commit()


def test_retire_dataset_rows_dry_run_removes_nothing(postgresql_conn, dataset_rows):
    counts = retire_dataset_rows(postgresql_conn, RETIRED, dry_run=True)

    assert counts == {"entity": 2, "old_entity": 2, "entity_subdivided": 2}
    for table in counts:
        assert count_rows(postgresql_conn, table, RETIRED) == 2


def test_retire_dataset_rows_removes_only_that_dataset(postgresql_conn, dataset_rows):
    counts = retire_dataset_rows(postgresql_conn, RETIRED, dry_run=False)

    assert counts == {"entity": 2, "old_entity": 2, "entity_subdivided": 2}
    for table in counts:
        assert count_rows(postgresql_conn, table, RETIRED) == 0
        assert count_rows(postgresql_conn, table, OTHER) == 1


def test_retire_dataset_rows_with_no_rows(postgresql_conn, dataset_rows):
    counts = retire_dataset_rows(postgresql_conn, "not-loaded-dataset", dry_run=False)

    assert counts == {"entity": 0, "old_entity": 0, "entity_subdivided": 0}
    for table in counts:
        assert count_rows(postgresql_conn, table, RETIRED) == 2


@pytest.mark.parametrize(
    "value, expected",
    [
        ("false", False),
        ("False", False),
        (" false ", False),
        ("true", True),
        ("", True),
        ("no", True),
        ("flase", True),
    ],
)
def test_is_dry_run(value, expected):
    assert is_dry_run(value) is expected
