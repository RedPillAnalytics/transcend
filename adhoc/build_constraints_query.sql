var p_table VARCHAR2(30)
var p_tablespace VARCHAR2(30)
var p_source_table VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_constraint_regexp VARCHAR2(30)
var p_constraint_type VARCHAR2(30)
var p_seg_attributes VARCHAR2 (3)
var l_targ_part VARCHAR2(3)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)

EXEC :p_table := 'test_stg'
     EXEC :p_tablespace := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :p_source_table := 'test_dim';
EXEC :p_source_owner := 'stewart';
EXEC :p_constraint_type := NULL;
EXEC :p_seg_attributes := 'no';
EXEC :l_targ_part := 'no';
EXEC :p_owner := 'stewart';
EXEC :p_table := 'test_dim';



SELECT
       -- this is the constraint name used if CON_RENAME (formulated below) already exists
       -- this is only used in the case of an exception
       -- this can be seen in debug mode
       :p_table
       || '_'
       || con_e_ext
       -- the rank function gives us a unique number to use for each index with a specific extension
       -- gives us something like UK1 or UK2
       || rank( ) OVER( partition BY con_e_ext ORDER BY constraint_name )
       con_e_rename,
       constraint_ddl, owner, constraint_name, con_rename, constraint_type,
       index_owner, index_name
  FROM ( SELECT regexp_replace
                ( regexp_replace
                  
                  -- different DBMS_METADATA function is used for referential integrity constraints
                  (    dbms_metadata.get_ddl
                    ( CASE constraint_type
                      WHEN 'R'
                      THEN 'REF_CONSTRAINT'
                      ELSE 'CONSTRAINT'
                      END,
                      constraint_name,
                      owner
                    ),
                    CASE
                    -- target is not partitioned and no tablespace provided
                    WHEN :l_targ_part = 'NO' AND :p_tablespace IS NULL
                    -- remove all partitioning and the local keyword
                    THEN '(\(\s*partition.+\))|local'
                    -- target is not partitioned but tablespace is provided
                    WHEN :l_targ_part = 'NO' AND :p_tablespace IS NOT NULL
                    -- strip out partitioned, local keyword and tablespace clause
                    THEN '(\(\s*partition.+\))|local|(tablespace)\s*[^ ]+'
                    -- target is partitioned and tablespace is provided
                    WHEN :l_targ_part = 'YES' AND :p_tablespace IS NOT NULL
                    -- strip out partitioning, keep local keyword and remove tablespace clause
                    THEN '(\(\s*partition.+\))|(tablespace)\s*[^ ]+'
                    ELSE NULL
                    END,
                    NULL,
                    1,
                    0,
                    'in'
                  ),
                  
                  -- TABLESPACE clause cannot come after the ENABLE|DISABLE keyword, so I need to place it before
                  '(\s+)(enable|disable)(\s*)$',
                  CASE
                  -- IF tablespace is provided, tack it on the end
                  WHEN td_core.get_yn_ind( :p_seg_attributes ) = 'yes'
                  AND :p_tablespace IS NOT NULL
                  AND constraint_type IN( 'P', 'U' )
                  THEN '\1TABLESPACE ' || :p_tablespace || '\1\2'
                  ELSE '\1\2\3'
                  END,
                  1,
                  0,
                  'i'
                ) constraint_ddl,
                owner, constraint_name, constraint_type, index_owner,
                index_name,
                
                -- this is the constraint name used with the first attempt
                -- this can be seen in debug mode
                regexp_replace( constraint_name,
                                :p_source_table,
                                :p_table,
                                1,
                                0,
                                'i'
                              ) con_rename,
                CASE constraint_type
                -- devise a specific constraint extention based on information about it
                WHEN 'R'
                THEN 'F'
                ELSE constraint_type || 'K'
                END con_e_ext
           FROM dba_constraints
          WHERE (table_name = upper( :p_source_table )
		  AND owner = upper( :p_source_owner )
		  AND REGEXP_LIKE( constraint_name,
				   nvl( :p_constraint_regexp, '.' ),
				   'i'
				 )
		  AND REGEXP_LIKE( constraint_type, nvl( :p_constraint_type, '.' ), 'i' ))
	     OR (constraint_type = 'R'
		  AND r_constraint_name IN( SELECT constraint_name
					      FROM dba_constraints
					     WHERE table_name = upper( :p_table )
					       AND owner = upper( :p_owner )
					       AND constraint_type = 'P' )))

