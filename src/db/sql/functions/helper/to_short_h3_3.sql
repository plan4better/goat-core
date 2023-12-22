/*From https://github.com/igor-suhorukov/openstreetmap_h3*/
DROP FUNCTION IF EXISTS basic.to_short_h3_3;
CREATE FUNCTION basic.to_short_h3_3(bigint) RETURNS smallint
AS $$ select ($1 & 'x000ffff000000000'::bit(64)::bigint>>36)::smallint;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE;