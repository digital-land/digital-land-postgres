\copy entity(entity,name,entry_date,start_date,end_date,dataset,json,organisation_entity,prefix,reference,typology,geojson,geometry,point) FROM './exported_entities.tsv' WITH (FORMAT CSV, HEADER, DELIMITER '|', FORCE_NULL(name,entry_date,start_date,end_date,dataset,organisation_entity,prefix,reference,typology,geojson,geometry,point))