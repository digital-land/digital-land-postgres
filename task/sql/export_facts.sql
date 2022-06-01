.headers on
.mode csv
.separator |

SELECT
    f.fact as fact,
    f.entity as entity,
    f.field as field,
    nullif(f.value, "") AS value,
    nullif(f.reference_entity, "") AS reference_entity,
    nullif(f.entry_date, "") AS entry_date,
    nullif(f.start_date, "") AS start_date,
    nullif(f.end_date, "") AS end_date
FROM fact f;