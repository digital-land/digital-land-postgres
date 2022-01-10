.headers on
.mode csv
.separator |
.output exported_digital-land.csv

SELECT
    d.dataset as dataset,
    nullif(d.name, "") as name,
    nullif(d.entry_date, "") as entry_date,
    nullif(d.start_date, "") as start_date,
    nullif(d.end_date, "") as end_date,
    nullif(d.collection, "") as collection,
    nullif(d.description, "") as description,
    nullif(d.key_field, "") as key_field,
    nullif(d.paint_options, "") as paint_options,
    nullif(d.plural, "") as plural,
    nullif(d.prefix, "") as prefix,
    nullif(d.text, "") as text,
    nullif(d.typology, "") as typology,
    nullif(d.wikidata, "") as wikidata,
    nullif(d.wikipedia, "") as wikipedia,
    "{" || GROUP_CONCAT(dt.theme, ',') || "}" as dataset_themes
FROM dataset d, dataset_theme dt
WHERE d.dataset = dt.dataset
GROUP BY d.dataset;
