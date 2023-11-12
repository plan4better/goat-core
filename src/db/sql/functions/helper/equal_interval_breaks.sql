DROP FUNCTION IF EXISTS basic.equal_interval_breaks; 
CREATE OR REPLACE FUNCTION basic.equal_interval_breaks(table_name text, column_name text, where_filter TEXT, breaks integer)
RETURNS JSONB
LANGUAGE plpgsql
AS $function$
DECLARE 
    result JSONB;
    min_val FLOAT;
    max_val FLOAT;
   	mean_val FLOAT; 
    interval_size FLOAT;
BEGIN
    -- Compute the min, max, and interval size directly
    EXECUTE format(
        'SELECT avg(%I), min(%I), max(%I), (max(%I) - min(%I)) / %L FROM %s WHERE %s',
        column_name, column_name, column_name, column_name, column_name, breaks, table_name, where_filter
    ) INTO mean_val, min_val, max_val, interval_size;

    -- Build the JSONB object with the computed breaks
    SELECT JSONB_BUILD_OBJECT('mean', mean_val, 'min', min_val, 'max', max_val, 'breaks', array_agg(computed_breaks))
    INTO result
    FROM (
        SELECT min_val + generate_series(1, breaks) * interval_size AS computed_breaks
    ) b;

    RETURN result;
END;
$function$;
/*
SELECT basic.equal_interval_breaks('user_data.point_744e4fd1685c495c8b02efebce875359', 'integer_attr1', 'layer_id = ''ac9cdd4f-8712-459b-bfb8-3e4664c48abb''', 5)
*/