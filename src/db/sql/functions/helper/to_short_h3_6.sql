
CREATE OR REPLACE FUNCTION basic.to_short_h3_6(bigint) RETURNS integer
AS $$ select ($1 & 'x000fffffff000000'::bit(64)::bigint>>24)::bit(32)::int;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION basic.to_short_h3_9(bigint) RETURNS bigint
AS $$ select ($1 & 'x00ffffffffff0000'::bit(64)::bigint>>16)::bit(64)::bigint;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION basic.to_short_h3_10(bigint) RETURNS bigint
AS $$ select ($1 & 'x00fffffffffff000'::bit(64)::bigint>>12)::bit(64)::bigint;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;