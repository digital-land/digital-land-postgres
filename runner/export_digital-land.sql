.headers off
.mode csv
.separator |
.output exported_digital-land.tsv

SELECT
    d.dataset,
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
    nullif(d.wikipedia, "") as wikipedia
FROM dataset d;
