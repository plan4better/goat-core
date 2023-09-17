CREATE OR REPLACE FUNCTION basic.count_public_transport_services_station(
    start_time interval,
    end_time interval,
    weekday integer,
    reference_area_sql text)
RETURNS TABLE(stop_id text, stop_name text, trip_cnt jsonb, geom geometry, trip_ids jsonb)
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Create temporary table and execute dynamic SQL
    EXECUTE format('DROP TABLE IF EXISTS buffer_geom; CREATE TEMP TABLE buffer_geom AS %s; CREATE INDEX buffer_geom_idx ON buffer_geom USING GIST(geom);', reference_area_sql);

    RETURN QUERY 
    WITH parent_stations AS (
        SELECT count(*) cnt_children, st.parent_station
        FROM gtfs.stops_optimized st, buffer_geom b
        WHERE ST_Intersects(st.geom, b.geom)
        AND st.location_type IS NULL
        AND st.geom && b.geom
        GROUP BY st.parent_station 
    ),
    g AS (
        SELECT c.parent_station, j.route_type, cnt AS cnt, j.trip_ids
        FROM parent_stations c  
        CROSS JOIN LATERAL (
            SELECT t.route_type, SUM(weekdays[weekday]::integer) cnt, ARRAY_AGG(trip_id) AS trip_ids  
            FROM gtfs.stop_times_optimized t, gtfs.stops_optimized s  
            WHERE t.stop_id = s.stop_id
            AND s.parent_station = c.parent_station
            AND t.arrival_time BETWEEN start_time AND end_time
            AND weekdays[weekday] = True
            GROUP BY t.route_type 
        ) j 
    ),
    o AS (
        SELECT g.parent_station, jsonb_object_agg(route_type, cnt) AS trip_cnt, jsonb_object_agg(route_type, g.trip_ids) AS trip_ids  
        FROM g 
        WHERE cnt <> 0
        GROUP BY parent_station
    )
    SELECT s.stop_id, s.stop_name, o.trip_cnt, s.geom, o.trip_ids  
    FROM o, gtfs.stops_optimized s 
    WHERE o.parent_station = s.stop_id;
END;
$function$;

/*
SELECT *
FROM basic.count_public_transport_services_station('06:00','20:00', 1, 'SELECT geom
FROM user_data.polygon_744e4fd1685c495c8b02efebce875359') s
*/


SELECT *
FROM basic.count_public_transport_services_station('06:00','20:00', 1, 
'SELECT names AS name, geometry AS geom FROM basic.poi_overture') s