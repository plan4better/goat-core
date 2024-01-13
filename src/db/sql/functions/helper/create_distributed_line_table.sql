DROP FUNCTION IF EXISTS basic.create_distributed_line_table; 
CREATE OR REPLACE FUNCTION basic.create_distributed_line_table(input_table text, relevant_columns text, where_filter text, result_table_name text)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN

    -- Check if query is empty
    IF where_filter = '' THEN
        where_filter = 'WHERE ';
    ELSE
        where_filter = REPLACE(where_filter, input_table, 'l')  || ' AND';
    END IF;
   

    -- Create empty distributed table 
   EXECUTE format(
        'DROP TABLE IF EXISTS %s; 
		CREATE TABLE %s AS 
		SELECT NULL::integer AS pk_id, %s, geom, NULL::INTEGER AS h3_3 FROM %s LIMIT 0;', 
        result_table_name, result_table_name, relevant_columns, input_table
    );	
    -- Make table distributed
   PERFORM create_distributed_table(result_table_name, 'h3_3'); 
   
   	-- Insert feature that are within one h3_3 grid 
   EXECUTE format(
        'INSERT INTO %s
        SELECT ROW_NUMBER() OVER() pk_id, %s, l.geom, h.h3_3
        FROM %s l, basic.h3_grid_resolution_3 h 
        %s
        ST_Intersects(l.geom, h.geom)
        AND ST_WITHIN(l.geom, h.geom);', result_table_name, relevant_columns, input_table, where_filter
   ); 

    -- Insert all where the lines intersect the border of the h3 grid and clip them using intersection.
    EXECUTE format(
        'INSERT INTO %s 
        SELECT ROW_NUMBER() OVER() id, %s, ST_Intersection(l.geom, h.geom) AS geom, h.h3_3
        FROM %s l, basic.h3_grid_resolution_3 h 
        %s
        ST_Intersects(l.geom, h.geom)
        AND ST_Intersects(l.geom, ST_ExteriorRing(h.geom));', result_table_name, relevant_columns, input_table, where_filter 
    ); 
	
   -- Add indices
   	EXECUTE format('ALTER TABLE %s ADD PRIMARY KEY (h3_3, pk_id);', result_table_name);
   	EXECUTE format('CREATE INDEX ON %s USING GIST(h3_3, geom);', result_table_name);

END;
$function$ 
PARALLEL SAFE;

/*
SELECT basic.create_distributed_line_table(
'test_user_data.line_744e4fd1685c495c8b02efebce875359', 'text_attr1', 'WHERE text_attr1 = ''Im Anspann''', 
'temporal.test_lines')
*/