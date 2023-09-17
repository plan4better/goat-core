CREATE OR REPLACE FUNCTION basic.oev_gueteklasse_buffer(
    _classification jsonb
)
RETURNS TABLE (_class integer, geom geometry) 
AS $$
BEGIN

	DROP TABLE IF EXISTS temp_buffered_stations; 
	CREATE TEMP TABLE temp_buffered_stations AS 
	SELECT s.stop_id, ST_BUFFER(s.geom::geography, j.KEY::integer)::geometry AS geom, j.KEY::integer buffer_size, REPLACE(j.value::TEXT, '"', '')::integer AS _class
	FROM basic.stations s, LATERAL jsonb_each(_classification -> s._class) j
	WHERE s."_class" <> 'no_class'; 
	CREATE INDEX ON temp_buffered_stations USING GIST(geom); 
	
	DROP TABLE IF EXISTS temp_union_buffer;
	CREATE TEMP TABLE temp_union_buffer AS 
	WITH clustered_buffer AS 
	(
		SELECT s.geom, s._class, ST_ClusterDBSCAN(geom, eps := 0, minpoints := 1) OVER (PARTITION BY _class) AS cluster_id
		FROM temp_buffered_stations s
	)
	SELECT b._class, ST_UNION(b.geom) AS geom 
	FROM clustered_buffer b
	WHERE cluster_id IS NOT NULL 
	GROUP BY b._class, cluster_id 
	UNION ALL 
	SELECT b._class, c.geom
	FROM clustered_buffer c
	WHERE cluster_id IS NULL;
	CREATE INDEX ON temp_buffered_stations USING GIST(geom); 
	
	RETURN QUERY SELECT CASE WHEN j.geom IS NULL THEN a.geom ELSE j.geom END AS geom, a._class  
	FROM temp_union_buffer a
	LEFT JOIN LATERAL 
	(
		SELECT ST_DIFFERENCE(a.geom, c.geom) AS geom 
		FROM (
			SELECT ST_UNION(b.geom) geom 
			FROM temp_union_buffer b
			WHERE a._class > b._class
			AND ST_Intersects(a.geom, b.geom)
		) c
	) j ON TRUE;

END;
$$ LANGUAGE plpgsql;