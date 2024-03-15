DROP FUNCTION IF EXISTS basic.oev_guetklasse_station_category;
CREATE OR REPLACE FUNCTION basic.oev_guetklasse_station_category(_child_count smallint, _station jsonb, _station_config jsonb, _start_time numeric, _end_time numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _station_groups text[] := '{}';
    _station_group_trip_count numeric := 0;
    _route_type text;
    _trip_count numeric;
    _station_group text;
    _station_group_trip_time_frequency numeric;
    _time_interval integer;
    _station_category text;
    _key text;
    _value numeric;
    _time_window NUMERIC;
BEGIN
	
	_time_window = (_end_time - _start_time) / 60;

    -- Iterating through the JSONB key-value pairs (assumes _station->>'trip_cnt' is a jsonb object)
    FOR _key, _value IN SELECT * FROM jsonb_each_text(_station) 
    LOOP
        _route_type := _key;
        _trip_count := _value::numeric;

        _station_group := _station_config->'groups'->>_route_type;
        IF _station_group IS NOT NULL THEN
            _station_groups := array_append(_station_groups, _station_group);
            _station_group_trip_count := _station_group_trip_count + _trip_count;
        END IF;
    END LOOP;
    
	IF _station_group_trip_count = 0 THEN
        RETURN JSONB_BUILD_OBJECT('_class', '999', 'frequency', 0);
    END IF;

    _station_group := (SELECT min(_group) FROM unnest(_station_groups) _group); -- Get minimum (highest priority)
    _station_group_trip_time_frequency := _time_window / (_station_group_trip_count / (CASE WHEN _child_count != 1 THEN 2 ELSE 1 END));
   	SELECT MIN(_class)
   	INTO _time_interval
	FROM (
		SELECT ROW_NUMBER() OVER() AS _class, _time
		FROM (SELECT jsonb_array_elements_text(_station_config->'time_frequency')::integer AS _time) x 
	) a
	WHERE _station_group_trip_time_frequency <= _time;

    IF _time_interval IS NULL THEN
        RETURN JSONB_BUILD_OBJECT('_class', '999', 'frequency', _station_group_trip_time_frequency);
    END IF;

    _station_category := _station_config->'categories'->_time_interval-2 ->>_station_group;

    IF _station_category IS NULL THEN
        RETURN JSONB_BUILD_OBJECT('_class', '999', 'frequency', _station_group_trip_time_frequency);
    ELSE
        RETURN JSONB_BUILD_OBJECT('_class', _station_category, 'frequency', _station_group_trip_time_frequency);
    END IF;
END;
$function$
PARALLEL SAFE;