SET termout off

COLUMN constraint_name format a30

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
var p_partname VARCHAR2(30)
var p_basis VARCHAR2(10)
var l_part_position NUMBER

EXEC :p_tablespace := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'whstage';
EXEC :p_table := 'customer_scd';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'customer_dim';
EXEC :p_constraint_type := NULL;
EXEC :p_seg_attributes := 'no';
EXEC :l_targ_part := 'no';
EXEC :p_partname := NULL;
EXEC :p_basis := 'all';
EXEC :l_part_position := 10;

EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SEGMENT_ATTRIBUTES',CASE LOWER( :p_seg_attributes ) WHEN 'yes' THEN TRUE ELSE FALSE END );

SET termout on
-- this case statement uses GENERIC_CON column to determine the final index name
-- IF we are using a generic name, then perform the replace
SELECT * FROM (SELECT UPPER( :p_owner ) constraint_owner,
                  CASE generic_con
                     WHEN 'Y'
                        THEN con_rename_adj
                     ELSE con_rename
                  END constraint_name, owner source_owner, table_name, constraint_name source_constraint,
                  constraint_type, index_owner, index_name,
                  CASE generic_con
                     WHEN 'Y'
                        THEN REGEXP_REPLACE( constraint_ddl,
                                             '(\."?)(\w)+(")?( on)',
                                             '.' || con_rename_adj || ' \4',
                                             1,
                                             0,
                                             'i'
                                           )
                     ELSE constraint_ddl
                  END constraint_ddl,
                     
                     -- this column was added for the REPLACE_TABLE procedure
                     -- IN that procedure, after cloning the indexes, the table is renamed
                     -- we have to rename the indexes back to their original names
                     ' alter constraint '
                  || owner
                  || '.'
                  || CASE generic_con
                        WHEN 'Y'
                           THEN con_rename_adj
                        ELSE con_rename
                     END
                  || ' rename to '
                  || constraint_name rename_ddl,
                     
                     -- this column was added for the REPLACE_TABLE procedure
                     -- IN that procedure, after cloning the indexes, the table is renamed
                     -- we have to rename the indexes back to their original names
                     'Constraint '
                  || owner
                  || '.'
                  || CASE generic_con
                        WHEN 'Y'
                           THEN con_rename_adj
                        ELSE con_rename
                     END
                  || ' renamed to '
                  || constraint_name rename_msg,
                          CASE
                             WHEN REGEXP_LIKE( 'table|all', :p_basis, 'i' )
                                THEN 'Y'
                             ELSE 'N'
                          END include
            FROM ( SELECT
                          -- IF con_rename already exists (constructed below), then we will try to rename the constraint to something generic
                          -- this name will only be used when con_rename name already exists
                          UPPER(    SUBSTR( :p_table, 1, 24 )
                                 || '_'
                                 || con_ext
                                 -- rank function gives us the constraint number by specific constraint extension (formulated below)
                                 || RANK( ) OVER( PARTITION BY con_ext ORDER BY constraint_name )
                               ) con_rename_adj,
                          REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( constraint_ddl,
                                                                          '(alter constraint).+',
                                                                          NULL,
                                                                          1,
                                                                          0,
                                                                          'i'
                                                                        ),
                                                             '(\.|constraint +)("?)('
                                                          || UPPER( :p_source_table )
                                                          || ')(\w*)("?)',
                                                          '\1' || UPPER( :p_table ) || '\4',
                                                          1,
                                                          0,
                                                          'i'
                                                        ),
                                          '(table )(")?(' || con.owner || ')("?\.)',
                                          '\1' || UPPER( :p_owner ) || '.',
                                          1,
                                          0,
                                          'i'
                                        ) constraint_ddl,
                          con.owner, table_name, constraint_name, con_rename, index_owner, index_name, con_ext,
                          constraint_type,
                          
                          -- this case expression determines whether to use the standard renamed constraint name
                          -- OR whether to use the generic constraint name based on table name
                          -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                          -- IF we match, then we need to use the generic constraint name
                          CASE
                             WHEN( ao.object_name IS NULL AND LENGTH( con_rename ) < 31 )
                                THEN 'N'
                             ELSE 'Y'
                          END generic_con, object_name
                    FROM ( SELECT    REGEXP_REPLACE
                                        
                                        -- dbms_metadata pulls the metadata for the source object out of the dictionary
                                     (    DBMS_METADATA.get_ddl( CASE constraint_type
                                                                    WHEN 'R'
                                                                       THEN 'REF_CONSTRAINT'
                                                                    ELSE 'CONSTRAINT'
                                                                 END,
                                                                 constraint_name,
                                                                 ac.owner
                                                               ),
                                          -- this CASE expression determines whether to strip partitioning information and tablespace information
                                          -- TABLESPACE desisions are based on the :p_TABLESPACE parameter
                                          -- partitioning decisions are based on the structure of the target table
                                          CASE
                                             -- target is not partitioned and neither :p_TABLESPACE or :p_PARTNAME are provided
                                          WHEN :l_targ_part = 'NO' AND :p_tablespace IS NULL AND :p_partname IS NULL
                                                -- remove all partitioning and the local keyword
                                          THEN '\s*(\(\s*partition.+\))|local\s*'
                                             -- target is not partitioned but :p_TABLESPACE or :p_PARTNAME is provided
                                          WHEN :l_targ_part = 'NO'
                                          AND ( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                                -- strip out partitioned info and local keyword and tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                             -- target is partitioned and :p_TABLESPACE or :p_PARTNAME is provided
                                          WHEN :l_targ_part = 'YES'
                                          AND ( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                                -- strip out partitioned info keeping local keyword and remove tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                             -- target is partitioned
                                             -- :p_tablespace IS NULL
                                             -- :p_partname IS NULL
                                          WHEN :l_targ_part = 'YES' AND :p_tablespace IS NULL AND :p_partname IS NULL
                                                -- leave partitioning and tablespace information as it is
                                                -- this implies a one-to-one mapping of partitioned names from source to target
                                          THEN NULL
                                          END,
                                          ' ',
                                          1,
                                          0,
                                          'in'
                                        )
                                  || CASE
                                        -- IF 'default' is passed, then use the users default tablespace
                                        -- a non-null value for :p_tablespace already stripped all tablespace information above
                                        -- now just need to not put in the 'TABLESPACE' information here
                                     WHEN LOWER( :p_tablespace ) = 'default'
                                           THEN NULL
                                        -- IF :p_TABLESPACE is provided, then previous tablespace information was stripped (above)
                                        -- now we can just tack the new tablespace information on the end
                                     WHEN :p_tablespace IS NOT NULL
                                           THEN ' TABLESPACE ' || UPPER( :p_tablespace )
                                        WHEN :p_partname IS NOT NULL
                                           THEN    ' TABLESPACE '
                                                || NVL( ai.tablespace_name,
                                                        ( SELECT tablespace_name
                                                           FROM all_ind_partitions
                                                          WHERE index_name = ac.index_name
                                                            AND index_owner = ac.owner
                                                            AND partition_position = :l_part_position )
                                                      )
                                        ELSE NULL
                                     END constraint_ddl,
                                  ac.owner, ac.table_name, ac.constraint_name, ac.index_owner, ac.index_name,
                                  
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
                                  END con_ext, constraint_type
                            FROM all_constraints ac LEFT JOIN all_indexes ai
                                 ON ac.index_owner = ai.owner AND ac.index_name = ai.index_name
                           WHERE ac.table_name = UPPER( :p_source_table )
                             AND ac.owner = UPPER( :p_source_owner )
                             AND REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
                             AND REGEXP_LIKE( constraint_type, NVL( :p_constraint_type, '.' ), 'i' )) con
                         LEFT JOIN
                         all_objects ao
                         ON ao.object_name = con.con_rename AND ao.owner = UPPER( :p_owner ) AND object_type = 'INDEX')
			  UNION ALL
			  SELECT UPPER( :p_owner ) constraint_owner,
                  CASE generic_con
                     WHEN 'Y'
                        THEN con_rename_adj
                     ELSE con_rename
                  END constraint_name, owner source_owner, table_name, constraint_name source_constraint,
                  constraint_type, index_owner, index_name,
                  CASE generic_con
                     WHEN 'Y'
                        THEN REGEXP_REPLACE( constraint_ddl,
                                             '(\."?)(\w)+(")?( on)',
                                             '.' || con_rename_adj || ' \4',
                                             1,
                                             0,
                                             'i'
                                           )
                     ELSE constraint_ddl
                  END constraint_ddl,
                     
                     -- this column was added for the REPLACE_TABLE procedure
                     -- IN that procedure, after cloning the indexes, the table is renamed
                     -- we have to rename the indexes back to their original names
                     ' alter constraint '
                  || owner
                  || '.'
                  || CASE generic_con
                        WHEN 'Y'
                           THEN con_rename_adj
                        ELSE con_rename
                     END
                  || ' rename to '
                  || constraint_name rename_ddl,
                     
                     -- this column was added for the REPLACE_TABLE procedure
                     -- IN that procedure, after cloning the indexes, the table is renamed
                     -- we have to rename the indexes back to their original names
                     'Constraint '
                  || owner
                  || '.'
                  || CASE generic_con
                        WHEN 'Y'
                           THEN con_rename_adj
                        ELSE con_rename
                     END
                  || ' renamed to '
       || constraint_name rename_msg,
                          CASE
                             WHEN REGEXP_LIKE( 'reference|all', :p_basis, 'i' )
                                THEN 'Y'
                             ELSE 'N'
                          END include
            FROM ( SELECT
                          -- IF con_rename already exists (constructed below), then we will try to rename the constraint to something generic
                          -- this name will only be used when con_rename name already exists
                          UPPER(    SUBSTR( :p_table, 1, 24 )
                                 || '_'
                                 || con_ext
                                 -- rank function gives us the constraint number by specific constraint extension (formulated below)
                                 || RANK( ) OVER( PARTITION BY con_ext ORDER BY constraint_name )
                               ) con_rename_adj,
                          REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( constraint_ddl,
                                                                          '(alter constraint).+',
                                                                          NULL,
                                                                          1,
                                                                          0,
                                                                          'i'
                                                                        ),
                                                             '(\.|constraint +)("?)('
                                                          || UPPER( :p_source_table )
                                                          || ')(\w*)("?)',
                                                          '\1' || UPPER( :p_table ) || '\4',
                                                          1,
                                                          0,
                                                          'i'
                                                        ),
                                          '(table )(")?(' || con.owner || ')("?\.)',
                                          '\1' || UPPER( :p_owner ) || '.',
                                          1,
                                          0,
                                          'i'
                                        ) constraint_ddl,
                          con.owner, table_name, constraint_name, con_rename, index_owner, index_name, con_ext,
                          constraint_type,
                          
                          -- this case expression determines whether to use the standard renamed constraint name
                          -- OR whether to use the generic constraint name based on table name
                          -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                          -- IF we match, then we need to use the generic constraint name
                          CASE
                             WHEN( ao.object_name IS NULL AND LENGTH( con_rename ) < 31 )
                                THEN 'N'
                             ELSE 'Y'
                          END generic_con, object_name
                    FROM ( SELECT  DBMS_METADATA.get_ddl( CASE constraint_type
                                                                    WHEN 'R'
                                                                       THEN 'REF_CONSTRAINT'
                                                                    ELSE 'CONSTRAINT'
                                                                 END,
                                                                 constraint_name,
                                                                 ac.owner
                                                               ) constraint_ddl,
                                  ac.owner, ac.table_name, ac.constraint_name, ac.index_owner, ac.index_name,
                                  
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
                                  END con_ext, constraint_type
                            FROM all_constraints ac
                           WHERE REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
			      AND constraint_type='R'
                      AND r_constraint_name IN(
						SELECT constraint_name
						  FROM all_constraints
						 WHERE table_name = UPPER( :p_source_table )
						   AND owner = UPPER( :p_source_owner )
						   AND constraint_type = 'P' )) con
                         LEFT JOIN
                         all_objects ao
                         ON ao.object_name = con.con_rename AND ao.owner = UPPER( :p_owner ) AND object_type = 'INDEX'
                 ))
			   WHERE include='Y'