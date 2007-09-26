var p_owner VARCHAR2(30)
var p_object VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_source_object VARCHAR2(30)
var p_grant_regexp VARCHAR2(30)

EXEC :p_owner := 'whdata';
EXEC :p_object := 'ar_transaction_pex';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_object := 'ar_transaction_fact';
EXEC :p_grant_regexp := NULL;


SELECT *
  FROM ( SELECT ( REGEXP_REPLACE( 
				  REGEXP_REPLACE( 
						  DBMS_METADATA.get_dependent_ddl( 'OBJECT_GRANT',
									 object_name,
									 owner
								       ),
						  '(\."?)('
						  || UPPER( :p_source_object )
						  || ')(\w*)("?)',
						  '.' || UPPER( :p_object ) || '\3',
						  1,
						  0,
						  'i'
						),
				  '(")?(' || upper( :p_source_owner ) || ')("?\.)',
				  UPPER( :p_owner ) || '.',
				  1,
				  0,
				  'i'
				)) ddl,
                owner object_owner, 
		object_name
           FROM all_objects ao
          WHERE object_name = UPPER( :p_source_object )
            AND owner = UPPER( :p_source_owner )
	    AND subobject_name IS NULL )
       -- USE an NVL'd regular expression to determine the specific indexes to work on
       -- when nothing is passed for :p_INDEX_TYPE, then that is the same as passing a wildcard
 WHERE REGEXP_LIKE( ddl, NVL( :p_grant_regexp, '.' ), 'i' )