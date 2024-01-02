DROP FUNCTION IF EXISTS basic.create_distributed_point_table; 
CREATE OR REPLACE FUNCTION basic.create_distributed_point_table(input_table text, relevant_columns text, where_filter text, result_table_name text)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Create empty distributed table 
    EXECUTE format(
        'DROP TABLE IF EXISTS %s; CREATE TABLE %s AS SELECT %s, geom, NULL::INTEGER AS h3_3 FROM %s LIMIT 0;', 
        result_table_name, result_table_name, relevant_columns, input_table
    );	
    -- Make table distributed
    PERFORM create_distributed_table(result_table_name, 'h3_3'); 

    -- Assign h3 grid id to the points.
    EXECUTE format(
        'INSERT INTO %s 
        SELECT %s, geom, basic.to_short_h3_3(h3_lat_lng_to_cell(geom::point, 3)::bigint) AS h3_3
        FROM %s %s;', result_table_name, relevant_columns, input_table, where_filter 
    ); 

    -- Add GIST index 
    EXECUTE format('CREATE INDEX ON %s USING GIST(h3_3, geom)', result_table_name);

END;
$function$ 
PARALLEL SAFE;