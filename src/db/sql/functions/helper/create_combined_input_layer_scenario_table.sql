DROP FUNCTION IF EXISTS basic.create_combined_input_layer_scenario_table; 
CREATE OR REPLACE FUNCTION basic.create_combined_input_layer_scenario_table(
    input_table text, input_layer_project_id int, scenario_id text, additional_columns text,
    where_filter text, temp_table text
)
RETURNS SETOF void
LANGUAGE plpgsql
AS $function$
BEGIN

	-- Creates a temporary table combining features from an input layer and a specified scenario
	EXECUTE format(
		'CREATE TABLE %s AS 
        WITH scenario_features AS (
            SELECT sf.feature_id AS id, sf.geom, sf.edit_type %s
            FROM customer.scenario_scenario_feature ssf
            INNER JOIN customer.scenario_feature sf ON sf.id = ssf.scenario_feature_id
            WHERE ssf.scenario_id = %L
            AND sf.layer_project_id = %s
        )
            SELECT original_features.id, original_features.geom %s
            FROM (SELECT * FROM %s %s) original_features
            LEFT JOIN scenario_features ON original_features.id = scenario_features.id
            WHERE scenario_features.id IS NULL
        UNION ALL
            SELECT scenario_features.id, scenario_features.geom %s
            FROM scenario_features
            WHERE edit_type IN (''n'', ''m'');',
        temp_table, additional_columns, scenario_id, input_layer_project_id,
        additional_columns, input_table, where_filter, additional_columns
	);
    EXECUTE format(
		'CREATE INDEX ON %s USING GIST(geom);',
        temp_table
	);

END;
$function$ 
PARALLEL SAFE;
