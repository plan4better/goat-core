DROP FUNCTION IF EXISTS basic.create_distributed_polygon_table; 
CREATE OR REPLACE FUNCTION basic.create_distributed_polygon_table(input_sql text, max_vertices_polygon integer, result_table_name text)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN
	
	EXECUTE format(
		'DROP TABLE IF EXISTS polygons; CREATE TEMP TABLE polygons AS %s;', input_sql
	);
	-- Create subdivided polygon table
	DROP TABLE IF EXISTS polygons_subdivided; 
	CREATE TEMP TABLE polygons_subdivided AS 
	SELECT ST_SUBDIVIDE(geom, max_vertices_polygon) AS geom 
	FROM polygons; 
	CREATE INDEX ON polygons_subdivided USING GIST(geom); 
	
	-- Identify grids ids and their respective geometries
	DROP TABLE IF EXISTS h3_3_grids_uuid; 
	CREATE TEMP TABLE h3_3_grids_uuid AS 
	WITH h3_3_ids AS 
	(
		SELECT DISTINCT basic.fill_polygon_h3(geom, 3) AS h3_index
		FROM polygons 
	)
	SELECT to_short_h3_3(h3_index::bigint) AS h3_3, ST_SETSRID(h3_cell_to_boundary(h3_index)::geometry, 4326) AS geom, 
	ST_Exteriorring(ST_SETSRID(h3_cell_to_boundary(h3_index)::geometry, 4326)) AS border 
	FROM h3_3_ids;  
	CREATE INDEX ON h3_3_grids_uuid USING GIST(geom); 
	CREATE INDEX ON h3_3_grids_uuid USING GIST(border); 
	
	-- Create empty distributed table 
	EXECUTE format(
		'DROP TABLE IF EXISTS %s; CREATE TABLE %s (geom geometry, h3_3 integer);', 
		result_table_name, result_table_name
	);	
	-- Make table distributed
	PERFORM create_distributed_table(result_table_name, 'h3_3'); 

	-- Assign h3 grid id to the intersecting polygons. Split polygons add border where necessary.
	EXECUTE format(
		'INSERT INTO %s 
		SELECT ST_INTERSECTION(g.geom, s.geom) AS geom, g.h3_3 AS h3_3
		FROM h3_3_grids_uuid g, polygons_subdivided s 
		WHERE ST_Intersects(g.border, s.geom)
		UNION ALL 
		SELECT s.geom, g.h3_3
		FROM h3_3_grids_uuid g, polygons_subdivided s 
		WHERE ST_WITHIN(s.geom, g.geom)
		AND ST_Intersects(s.geom, g.geom)', 
		result_table_name
	); 

	-- Add GIST index 
	EXECUTE format('CREATE INDEX ON %s USING GIST(h3_3, geom)', result_table_name);

END;
$function$ 

/*
SELECT basic.create_distributed_polygon_table
(
	'SELECT *
	FROM user_data.polygon_744e4fd1685c495c8b02efebce875359
	WHERE text_attr1 LIKE ''DE%''
	AND float_attr1 = 3
	AND layer_id = ''097d6f2b-11a3-48bc-b592-1cf6e6c9e9a3''',
	30, 'temporal.test'
)
*/