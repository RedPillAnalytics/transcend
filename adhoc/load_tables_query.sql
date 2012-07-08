SET termout off
COLUMN disable_ddl format a130
COLUMN enable_ddl format a130
COLUMN target_name_old format a35

var p_owner VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_source_regexp VARCHAR2(100)
var p_suffix VARCHAR2(30)
var p_part_tabs VARCHAR2(3)
var p_source_type VARCHAR2(10)

EXEC :p_source_owner := 'testload';
EXEC :p_owner := 'testload2';
EXEC :p_source_regexp := '.';
EXEC :p_suffix := null;
EXEC :p_source_type := 'view|table';

SET termout on

SELECT *
  FROM ( SELECT owner source_owner,
	        object_name source_object,
	        object_type source_object_type,
                upper( CASE WHEN :p_suffix IS NULL THEN object_name ELSE regexp_replace( object_name, '(_[^_]+)$', NULL ) END ) target_name
	   FROM all_objects
          WHERE REGEXP_LIKE( object_type, :p_source_type, 'i' )
            AND REGEXP_LIKE( object_name, :p_source_regexp, 'i' )
            AND REGEXP_LIKE( object_name, CASE WHEN :p_suffix IS NULL THEN '.' ELSE '_' || :p_suffix || '$' END, 'i' )
            AND owner = upper ( :p_source_owner )
       ) s
  JOIN ( SELECT owner target_owner,
  		object_name target_name,
		object_type target_oject_type
  	   FROM all_objects
	  WHERE object_type IN ( 'TABLE' )
            AND owner = upper( :p_owner ) 
       ) t 
       USING (target_name)
