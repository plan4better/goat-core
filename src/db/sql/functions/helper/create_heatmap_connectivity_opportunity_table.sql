DROP FUNCTION IF EXISTS basic.create_heatmap_connectivity_opportunity_table; 
CREATE OR REPLACE FUNCTION basic.create_heatmap_connectivity_opportunity_table(
    input_table text, where_filter text, result_table_name text,
    grid_resolution int, append_existing boolean
)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN
    IF NOT append_existing THEN
        -- Create empty distributed table 
        EXECUTE format(
            'DROP TABLE IF EXISTS %s;
            CREATE TABLE %s (
                id int,
                h3_index h3index,
                h3_3 int
            );',
            result_table_name, result_table_name
        );	
        -- Make table distributed
        PERFORM create_distributed_table(result_table_name, 'h3_3');
    END IF;

    -- Produce h3 grid at specified resolution
    IF grid_resolution = 10 THEN
	    EXECUTE format(
	        'INSERT INTO %s 
	        SELECT input_table.id, h3_index, basic.to_short_h3_3(h3_lat_lng_to_cell(ST_Centroid(h3_boundary)::point, 3)::bigint) AS h3_3
	        FROM (SELECT * FROM %s %s) input_table,
	        LATERAL basic.fill_polygon_h3_10(input_table.geom);', result_table_name, input_table, where_filter 
	    );
	ELSIF grid_resolution = 9 THEN
		EXECUTE format(
	        'INSERT INTO %s 
	        SELECT input_table.id, h3_index, basic.to_short_h3_3(h3_lat_lng_to_cell(ST_Centroid(h3_boundary)::point, 3)::bigint) AS h3_3
	        FROM (SELECT * FROM %s %s) input_table,
	        LATERAL basic.fill_polygon_h3_9(input_table.geom);', result_table_name, input_table, where_filter 
	    );
    ELSIF grid_resolution = 8 THEN
		EXECUTE format(
	        'INSERT INTO %s 
	        SELECT input_table.id, h3_index, basic.to_short_h3_3(h3_lat_lng_to_cell(ST_Centroid(h3_boundary)::point, 3)::bigint) AS h3_3
	        FROM (SELECT * FROM %s %s) input_table,
	        LATERAL basic.fill_polygon_h3_8(input_table.geom);', result_table_name, input_table, where_filter 
	    );
	ELSE
		RAISE EXCEPTION 'Unsupported grid resolution specified';
	END IF;

    IF NOT append_existing THEN
        -- Add index 
        EXECUTE format('CREATE INDEX ON %s (h3_index, h3_3);', result_table_name);
    END IF;

END;
$function$ 
PARALLEL SAFE;
