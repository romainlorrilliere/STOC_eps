ALTER TABLE point ADD COLUMN geom93 geometry(Point,2154);
UPDATE point
SET geom93 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude_wgs84,latitude_wgs84),4326),2154);


ALTER TABLE carre ADD COLUMN points_geom93 geometry(Point,2154);
UPDATE carre
SET points_geom93 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude_median_wgs84,latitude_median_wgs84),4326),2154);


ALTER TABLE carre ADD COLUMN grid_geom93 geometry(Point,2154);
UPDATE carre
SET grid_geom93 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude_grid_wgs84,latitude_grid_wgs84),4326),2154);
