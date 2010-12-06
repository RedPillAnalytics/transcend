SET termout off
COLUMN disable_ddl format a130
COLUMN enable_ddl format a130

var p_owner VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_source_regexp VARCHAR2(100)
var p_suffix VARCHAR2(30)
var p_part_tabs VARCHAR2(3)

EXEC :p_source_owner := 'stage';
EXEC :p_owner := 'stewart';
EXEC :p_source_regexp := '^tst_.+_src$';
EXEC :p_suffix := 'src';

SET termout on

SELECT *
  FROM (SELECT owner source_owner,
	       object_name source_object,
	       object_type source_object_type,
	       upper( regexp_replace( object_name, '(.+)(_)(.+)$', '\1'||CASE WHEN :p_suffix IS NULL THEN NULL ELSE '_'||:p_suffix END )) target_name
	  FROM all_objects 
	 WHERE object_type IN ( 'TABLE', 'VIEW', 'SYNONYM' )) s
  JOIN ( SELECT owner target_owner,
  		object_name target_name,
		object_type target_oject_type
  	   FROM all_objects
	  WHERE object_type IN ( 'TABLE' ) ) t 
       USING (target_name)
 WHERE REGEXP_LIKE( source_object, :p_source_regexp, 'i' )
   AND source_owner = upper( :p_source_owner )
   AND target_owner = upper( :p_owner )
