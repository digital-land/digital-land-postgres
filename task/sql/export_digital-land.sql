.headers on
.mode csv
.separator |
.output exported_dataset.csv

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
    "{" || GROUP_CONCAT(dt.theme, ',') || "}" as themes
FROM dataset d, dataset_theme dt
WHERE d.dataset = dt.dataset
GROUP BY d.dataset;

.headers on
.mode csv
.separator |
.output exported_organisation.csv

SELECT
    o.organisation as organisation,
    nullif(o.name, "") as name,
    nullif(o.combined_authority, "") as combined_authority,
    nullif(o.entry_date, "") as entry_date,
    nullif(o.start_date, "") as start_date,
    nullif(o.end_date, "") as end_date,
    nullif(o.entity, "") as entity,
    nullif(o.local_authority_type, "") as local_authority_type,
    nullif(o.official_name, "") as official_name,
    nullif(o.region, "") as region,
    nullif(o.statistical_geography, "") as statistical_geography,
    nullif(o.website, "") as website
FROM organisation o;


.headers on
.mode csv
.separator |
.output exported_typology.csv

SELECT
    t.typology as typology,
    nullif(t.name, "") as name,
    nullif(t.description, "") as description,
    nullif(t.entry_date, "") as entry_date,
    nullif(t.start_date, "") as start_date,
    nullif(t.end_date, "") as end_date,
    nullif(t.plural, "") as plural,
    nullif(t.text, "") as text,
    nullif(t.wikidata, "") as wikidata,
    nullif(t.wikipedia, "") as wikipedia
FROM typology t;
