ALTER TABLE carrenat ADD COLUMN geom93 geometry(Point,2154);
UPDATE carrenat
SET geom93 = ST_Transform(ST_SetSRID(ST_MakePoint(long_lamb2e,lat_lamb2e),27582),2154);
