SET termout off
COLUMN disable_ddl format a130
COLUMN enable_ddl format a130

var p_owner VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_source_regexp VARCHAR2(100)
var p_suffix VARCHAR2(30)
var p_part_tabs VARCHAR2(3)

EXEC :p_owner := 'staging';
EXEC :p_source_owner := 'source_data';
EXEC :p_source_regexp := '^wps_.+_vw$';
EXEC :p_suffix := 't';
EXEC :p_part_tabs := 'no';

SET termout on

SELECT *
  FROM (SELECT owner source_owner,
	       object_name source_object,
	       object_type,
	       upper( regexp_replace( object_name, '(.+)(_)(.+)$', '\1'||CASE WHEN :p_suffix IS NULL THEN NULL ELSE '_'||:p_suffix END )) table_name
	  FROM all_objects ) s
  JOIN ( SELECT owner table_owner,
		table_name 
	   FROM all_tables ) t 
       USING (table_name)
 WHERE REGEXP_LIKE( source_object, :p_source_regexp, 'i' )
   AND source_owner = upper( :p_source_owner )
   AND table_owner = upper( :p_owner )
   AND s.object_type IN( 'TABLE', 'VIEW', 'SYNONYM' )