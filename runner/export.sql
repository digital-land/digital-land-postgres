.headers off
.mode csv
.separator |
.output exported_entities.tsv

SELECT
    e.entity,
    nullif(e.name, "") as name,
    nullif(e.entry_date, "") as entry_date,
    nullif(e.start_date, "") as start_data,
    nullif(e.end_date, "") as end_date,
    nullif(e.dataset, "") as dataset,
    nullif(e.json, "") as json,
    nullif(e.organisation_entity, "") as organisation_entity,
    nullif(e.prefix, "") as prefix,
    nullif(e.reference, "") as reference,
    nullif(e.typology, "") as typology,
    nullif(e.geojson, "") as geojson,
    nullif(e.geometry, "") as geometry,
    nullif(e.point, "") as point
FROM entity e;
