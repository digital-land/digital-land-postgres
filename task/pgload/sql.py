
# TODO - create digital land sql copy, upsert etc.

class EntitySQL:

    clone_table = (
        "CREATE TEMPORARY TABLE __temp_table AS (SELECT * FROM ENTITY LIMIT 0);"
    )

    copy = """
        COPY __temp_table (
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
        ) FROM STDIN WITH (
            FORMAT CSV, 
            HEADER, 
            DELIMITER '|', 
            FORCE_NULL(
                name,
                entry_date,
                start_date,
                end_date,
                dataset,
                organisation_entity,
                prefix,
                reference,
                typology,
                geojson,
                geometry,
                point))
    """

    upsert = """
        INSERT INTO entity
        SELECT *
        FROM __temp_table
        ON CONFLICT (entity) DO UPDATE
        SET name=EXCLUDED.name,
            entry_date=EXCLUDED.entry_date,
            start_date=EXCLUDED.start_date,
            end_date=EXCLUDED.end_date,
            dataset=EXCLUDED.dataset,
            json=EXCLUDED.json,
            organisation_entity=EXCLUDED.organisation_entity,
            prefix=EXCLUDED.prefix,
            reference=EXCLUDED.reference,
            typology=EXCLUDED.typology,
            geojson=EXCLUDED.geojson,
            geometry=EXCLUDED.geometry,
            point=EXCLUDED.point;
    """
