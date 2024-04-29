DROP FUNCTION IF EXISTS basic.create_heatmap_closest_average_opportunity_table; 
CREATE OR REPLACE FUNCTION basic.create_heatmap_closest_average_opportunity_table(
    input_table text, max_traveltime int, num_destinations int,
    where_filter text, result_table_name text, grid_resolution int,
    append_existing boolean
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
                max_traveltime smallint,
                num_destinations int,
                h3_3 int
            );',
            result_table_name, result_table_name
        );	
        -- Make table distributed
        PERFORM create_distributed_table(result_table_name, 'h3_3');
    END IF;

    -- Ensure grid resolution is supported
    IF grid_resolution NOT IN (10, 9, 8) THEN
		RAISE EXCEPTION 'Unsupported grid resolution specified';
	END IF;

    -- Produce h3 grid at specified resolution
    EXECUTE format(
        'INSERT INTO %s 
        SELECT id, h3_lat_lng_to_cell(geom::point, %s) AS h3_index, %s AS max_traveltime, %s AS num_destinations,
            basic.to_short_h3_3(h3_lat_lng_to_cell(geom::point, 3)::bigint) AS h3_3
        FROM (SELECT * FROM %s %s) input_table;', result_table_name, grid_resolution, max_traveltime, num_destinations, input_table, where_filter 
    );

    IF NOT append_existing THEN
        -- Add index 
        EXECUTE format('CREATE INDEX ON %s (h3_index, h3_3);', result_table_name);
    END IF;

END;
$function$ 
PARALLEL SAFE;
