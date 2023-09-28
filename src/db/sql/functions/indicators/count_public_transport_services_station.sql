DROP FUNCTION IF EXISTS basic.count_public_transport_services_station;
CREATE OR REPLACE FUNCTION basic.count_public_transport_services_station(
    start_time interval,
    end_time interval,
    weekday integer,
    reference_table_name TEXT
)
RETURNS TABLE(stop_id text, stop_name text, trip_cnt jsonb, geom geometry, trip_ids jsonb, h3_3 integer) 
LANGUAGE plpgsql
AS $function$
DECLARE
	table_parent_station TEXT; 
BEGIN
	-- Create the temp table name based on a uuid
	table_parent_station = 'temporal.' || '"' || REPLACE(uuid_generate_v4()::TEXT, '-', '') || '"';
	
    -- Create temporary table and execute dynamic SQL
	EXECUTE format(
		'DROP TABLE IF EXISTS %s;
		CREATE TABLE %s
		(
			parent_station TEXT,
			h3_3 integer 
		);', table_parent_station, table_parent_station
	);
	-- Distribute the table with parent stations
	PERFORM create_distributed_table(table_parent_station, 'h3_3');
	
	-- Get relevant stations
	EXECUTE format(
		'INSERT INTO %s
		SELECT st.parent_station, b.h3_3
	    FROM gtfs.stops st, %s b
	    WHERE ST_Intersects(st.geom, b.geom)
	    AND st.location_type IS NULL
	    AND st.geom && b.geom
	    AND st.h3_3 = b.h3_3
	    GROUP BY st.parent_station, b.h3_3',
	    table_parent_station, reference_table_name
	 );
  	
   	-- Count trips per station and transport mode in respective time interval 
    RETURN QUERY EXECUTE format(
		'WITH trip_cnt AS 
		(
		    SELECT c.parent_station, j.route_type, cnt AS cnt, j.trip_ids, c.h3_3
		    FROM %s c
		    CROSS JOIN LATERAL (
		        SELECT t.route_type, SUM(weekdays[$1]::integer) cnt, ARRAY_AGG(trip_id) AS trip_ids  
		        FROM gtfs.stop_times_optimized t, gtfs.stops s  
		        WHERE t.stop_id = s.stop_id
		        AND s.parent_station = c.parent_station
		        AND s.h3_3 = c.h3_3 
		        AND t.h3_3 = s.h3_3
		        AND t.arrival_time BETWEEN $2 AND $3
		        AND weekdays[$4] = True
		        GROUP BY t.route_type 
		    ) j 
		),
	    o AS (
	        SELECT parent_station, jsonb_object_agg(route_type, cnt) AS trip_cnt, jsonb_object_agg(route_type, g.trip_ids) AS trip_ids, h3_3  
	        FROM trip_cnt g 
	        WHERE cnt <> 0
	        GROUP BY parent_station, h3_3 
	    )
	    SELECT s.stop_id, s.stop_name, o.trip_cnt, s.geom, o.trip_ids, o.h3_3  
	    FROM o, gtfs.stops s 
	    WHERE o.parent_station = s.stop_id
	    AND s.h3_3 = o.h3_3', table_parent_station) USING weekday, start_time, end_time, weekday;
END;
$function$;

/*
SELECT *
FROM basic.count_public_transport_services_station('06:00','20:00', 1, 'SELECT geom
FROM user_data.polygon_744e4fd1685c495c8b02efebce875359') s
*/
