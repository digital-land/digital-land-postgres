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
    "{" || GROUP_CONCAT(dt.theme, ',') || "}" as themes,
    nullif(d.attribution, "") as attribution_id,
    nullif(d.licence, "") as licence_id,
    nullif(d.consideration, "") as consideration
FROM dataset d, dataset_theme dt
WHERE d.dataset = dt.dataset
GROUP BY d.dataset;

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

.output exported_dataset_collection.csv

SELECT
    nullif(source_pipeline.pipeline, "") as dataset_collection,
    nullif(resource.resource, "") as resource,
    nullif(resource.end_date, "") as resource_end_date,
    nullif(resource.entry_date, "") as resource_entry_date,
    max(resource.start_date) as last_updated,
    max(log.entry_date) as last_collection_attempt
FROM resource, resource_endpoint, source, source_pipeline, log
WHERE source_pipeline.pipeline IN (SELECT distinct(dataset) FROM DATASET)
  AND resource.resource = resource_endpoint.resource
  AND resource_endpoint.endpoint = source.endpoint
  AND source.source = source_pipeline.source
  AND source.endpoint = log.endpoint
GROUP BY source_pipeline.pipeline;


.output exported_dataset_publication.csv

SELECT
    source_pipeline.pipeline AS dataset_publication,
    COUNT(DISTINCT source.organisation) AS expected_publisher_count,
    COUNT(DISTINCT CASE WHEN source.endpoint != '' THEN source.organisation END) AS publisher_count
FROM source
    INNER JOIN source_pipeline
        ON source.source = source_pipeline.source
GROUP BY dataset_publication;


.output exported_lookup.csv

SELECT
    l.rowid as id,
    l.entity as entity,
    nullif(l.prefix, "") as prefix,
    nullif(l.reference, "") as reference,
    nullif(l.entry_date, "") as entry_date,
    nullif(l.start_date, "") as start_date,
    nullif(l.value, "") as value
FROM lookup l;


.output exported_attribution.csv

SELECT
    a.attribution as attribution,
    nullif(a.text, "") as text,
    nullif(a.entry_date, "") as entry_date,
    nullif(a.start_date, "") as start_date,
    nullif(a.end_date, "") as end_date
FROM attribution a;


.output exported_licence.csv

SELECT
    l.licence as licence,
    nullif(l.text, "") as text,
    nullif(l.entry_date, "") as entry_date,
    nullif(l.start_date, "") as start_date,
    nullif(l.end_date, "") as end_date
FROM licence l;
