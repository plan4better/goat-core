
DROP FUNCTION IF EXISTS basic.create_distributed_polygon_table; 
CREATE OR REPLACE FUNCTION basic.create_distributed_polygon_table(input_table text, relevant_columns text, where_filter text,
max_vertices_polygon integer, result_table_name text)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN

	-- Create temporary table for polygons
	EXECUTE format(
		'DROP TABLE IF EXISTS polygons; CREATE TEMP TABLE polygons AS SELECT %s, geom FROM %s %s;', relevant_columns, input_table, where_filter 
	);
	
	-- Create subdivided polygon table and add GIST index
	EXECUTE format(
		'DROP TABLE IF EXISTS polygons_subdivided; 
		CREATE TEMP TABLE polygons_subdivided AS 
		WITH splitted AS 
		(
			SELECT %s, ST_SUBDIVIDE(geom, %s) AS geom 
			FROM polygons
		)
		SELECT %s, 
		CASE WHEN ST_IsValid(geom) = TRUE
		THEN geom
		ELSE ST_MakeValid(geom)
		END AS geom 
		FROM polygons;', relevant_columns, max_vertices_polygon, relevant_columns
	);
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
		'DROP TABLE IF EXISTS %s; CREATE TABLE %s AS SELECT *, NULL::INTEGER AS h3_3 FROM polygons_subdivided LIMIT 0;', 
		result_table_name, result_table_name
	);	
	-- Make table distributed
	PERFORM create_distributed_table(result_table_name, 'h3_3'); 
	
	-- Assign h3 grid id to the intersecting polygons. Split polygons add border where necessary.
	EXECUTE format(
		'INSERT INTO %s 
		SELECT %s, ST_INTERSECTION(g.geom, s.geom) AS geom, g.h3_3 AS h3_3
		FROM h3_3_grids_uuid g, polygons_subdivided s 
		WHERE ST_Intersects(g.border, s.geom)
		UNION ALL 
		SELECT %s, s.geom, g.h3_3
		FROM h3_3_grids_uuid g, polygons_subdivided s 
		WHERE ST_WITHIN(s.geom, g.geom)
		AND ST_Intersects(s.geom, g.geom)', 
		result_table_name, relevant_columns, relevant_columns
	); 

	-- Add GIST index 
	EXECUTE format('CREATE INDEX ON %s USING GIST(h3_3, geom)', result_table_name);
END;
$function$ 
PARALLEL SAFE;

/*
EXPLAIN ANALYZE 
SELECT basic.create_distributed_polygon_table(
	'user_data.polygon_744e4fd1685c495c8b02efebce875359', 
	'text_attr1',
	'WHERE layer_id = ''4bdbed8f-4804-4913-9b42-c547e7be0fd5'' AND text_attr1=''Niedersachsen''',
	30,
	'temporal.test'
)
*/
