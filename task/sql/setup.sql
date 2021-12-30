CREATE EXTENSION IF NOT EXISTS postgis;

DROP INDEX IF EXISTS entity_index;
DROP INDEX IF EXISTS entity_geometry_geom_idx;
DROP INDEX IF EXISTS entity_point_geom_idx;

CREATE TABLE IF NOT EXISTS entity (
      entity BIGINT PRIMARY KEY,
      name TEXT,
      entry_date DATE,
      start_date DATE,
      end_date DATE,
      dataset TEXT,
      json JSONB,
      organisation_entity BIGINT,
      prefix TEXT,
      reference TEXT,
      typology TEXT,
      geojson JSONB,
      geometry GEOMETRY,
      point GEOMETRY
);
