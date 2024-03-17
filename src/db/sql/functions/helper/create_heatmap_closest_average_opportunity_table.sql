DROP FUNCTION IF EXISTS basic.create_heatmap_closest_average_opportunity_table; 
CREATE OR REPLACE FUNCTION basic.create_heatmap_closest_average_opportunity_table(
    input_table text, max_traveltime smallint, num_destinations int,
    where_filter text, result_table_name text, append_existing boolean
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

    -- Assign h3 grid id to the points.
    EXECUTE format(
        'INSERT INTO %s 
        SELECT id, h3_lat_lng_to_cell(geom::point, 10) AS h3_index, %s AS max_traveltime, %s AS num_destinations,
            basic.to_short_h3_3(h3_lat_lng_to_cell(geom::point, 3)::bigint) AS h3_3
        FROM (SELECT * FROM %s %s) input_table;', result_table_name, max_traveltime, num_destinations, input_table, where_filter 
    );

    IF NOT append_existing THEN
        -- Add index 
        EXECUTE format('CREATE INDEX ON %s (h3_index, h3_3);', result_table_name);
    END IF;

END;
$function$ 
PARALLEL SAFE;
