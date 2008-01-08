SET echo off
SET termout off

var p_table VARCHAR2(30)
var p_tablespace VARCHAR2(30)
var p_source_table VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_seg_attributes VARCHAR2(3)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_partitioning VARCHAR2(3)
var p_tablespace VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_owner := 'whdata';
EXEC :p_table := 'td$customer_dim';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'customer_dim';
EXEC :p_seg_attributes := 'no';
EXEC :p_partitioning := 'yes';
EXEC :p_tablespace := 'whimportd';

-- don't want any constraints pulled
dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'CONSTRAINTS', FALSE );
dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'REF_CONSTRAINTS', FALSE );
-- EXECUTE immediate doesn't like ";" on the end
dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SQLTERMINATOR', FALSE );
-- we need the segment attributes so things go where we want them to
dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', TRUE );
-- don't want all the other storage aspects though
dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'STORAGE', FALSE );

SET termout on
-- SELECT DDL into a variable
   SELECT 	  
	  -- this regular expression evaluates whether to use a modified version of the current constraint name
	  -- or a generic constraint_name based on the table name
	  -- this is only important when the table is an IOT
	  regexp_replace( table_ddl,
			  '(constraint )("?)(\w+)("?)',
                         '\1' || CASE generic_con
                            WHEN 'Y'
                               THEN con_rename_adj
                            ELSE con_rename
                         END,
                         1,
                         0,
                          'i') table_ddl,
	             -- this column was added for the REPLACE_TABLE procedure
            -- IN that procedure, after cloning the indexes, the table is renamed
            -- we have to rename the indexes back to their original names
            ' alter constraint '
         || source_owner
         || '.'
         || CASE generic_con
               WHEN 'Y'
                  THEN con_rename_adj
               ELSE con_rename
            END
         || ' rename to '
         || source_constraint rename_ddl,
            
            -- this column was added for the REPLACE_TABLE procedure
            -- IN that procedure, after cloning the indexes, the table is renamed
            -- we have to rename the indexes back to their original names
            'Constraint '
         || source_owner
         || '.'
         || CASE generic_con
               WHEN 'Y'
                  THEN con_rename_adj
               ELSE con_rename
            END
         || ' renamed to '
          || source_constraint rename_msg,
	  iot_type
     FROM ( SELECT
	  -- this regular expression evaluates P_PARTITIONING paramater and removes partitioning information if necessary
	  regexp_replace( table_ddl,
			  CASE
                               -- don't want partitioning
                          WHEN td_core.get_yn_ind( :p_partitioning ) = 'no'
                                  -- remove all partitioning
                            THEN '(\(\s*partition.+\))\s*|(partition by).+\)\s*'
                               ELSE NULL
                            END,
                            NULL,
                            1,
                            0,
                            'in'
                        ) table_ddl,
	  con_rename,
	  con_rename_adj,
		   iot_type,
		   source_owner,
		   source_constraint,
          -- this case expression determines whether to use the standard renamed constraint name
          -- OR whether to use the generic constraint name based on table name
          -- below we are right joining with USER_OBJECTS to see if the standard name is already used
          -- IF we match, then we need to use the generic constraint name
          CASE
          WHEN( constraint_name_confirm IS NULL AND LENGTH( con_rename ) < 31 )
          THEN 'N'
          ELSE 'Y'
          END generic_con
     FROM ( SELECT 
	  -- IF con_rename already exists (constructed below), then we will try to rename the constraint to something generic
          -- this name will only be used when con_rename name already exists
          UPPER(    SUBSTR( :p_table, 1, 24 ) || '_' || con_ext
                 || CASE constraint_type
                 WHEN 'P'
                 THEN NULL
                 ELSE RANK( ) OVER( PARTITION BY con_ext ORDER BY source_constraint )
                 END
		 
		 -- rank function gives us the constraint number by specific constraint extension (formulated below)
               ) con_rename_adj,
	  iot_type,
	  con_rename,
		   table_ddl,
		   constraint_name_confirm,
		   source_owner,
		   source_constraint 
     FROM (SELECT dbms_metadata.get_ddl( 'TABLE', table_name, owner ) table_ddl, 
		  iot_type,
		  owner source_owner,
		  constraint_name source_constraint,
		  constraint_type,
		  -- this is the constraint name that will be used if it doesn't already exist
                  -- basically, all cases of the previous table name are replaced with the new table name
                  UPPER( REGEXP_REPLACE( constraint_name,
                                         '(")?' || :p_source_table || '(")?',
                                         :p_table,
                                         1,
                                         0,
                                         'i'
                                       )
                       ) con_rename,
                  CASE constraint_type
                  -- devise a specific constraint extention based on information about it
                  WHEN 'R'
                  THEN 'F'
                  ELSE constraint_type || 'K'
                  END con_ext
	     FROM all_tables
		  -- joining here to get the primary key for the table (if it exists)
		  -- this is used to handle IOT's correctly
	     left JOIN all_constraints
		  USING (owner, table_name)
	    WHERE owner = upper( :p_source_owner ) AND table_name = upper( :p_source_table )
	      AND constraint_type='P') g1
	      left JOIN
		   -- joining here to see if the proposed constraint_name (con_rename) actually exists
		   (SELECT owner constraint_owner_confirm,
			   constraint_name constraint_name_confirm
		      FROM all_constraints) g2 
		   ON g1.con_rename = g2.constraint_name_confirm AND g2.constraint_owner_confirm = upper( :p_owner )))