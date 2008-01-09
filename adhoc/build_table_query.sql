SET echo off
SET termout off
COLUMN rename_msg format a50
COLUMN table_ddl format a100

VAR p_table VARCHAR2(30)
VAR p_tablespace VARCHAR2(30)
VAR p_source_table VARCHAR2(30)
VAR p_source_owner VARCHAR2(30)
VAR p_seg_attributes VARCHAR2(3)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
VAR p_partitioning VARCHAR2(3)
VAR p_tablespace VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_owner := 'whstage';
EXEC :p_table := 'td$customer_dim';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'customer_dim';
EXEC :p_seg_attributes := 'no';
EXEC :p_partitioning := 'yes';
EXEC :p_tablespace := 'default';

-- don't want any constraints pulled
EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'CONSTRAINTS', FALSE );
EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'REF_CONSTRAINTS', FALSE );
-- EXECUTE immediate doesn't like ";" on the end
EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SQLTERMINATOR', FALSE );
-- we need the segment attributes so things go where we want them to
EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', TRUE );
-- don't want all the other storage aspects though
EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'STORAGE', FALSE );

SET termout on
SELECT REGEXP_REPLACE(
                       -- replace the source table name with the target table name
                       REGEXP_REPLACE(
                                       -- this regular expression evaluates whether to use a modified version of the current constraint name
                                       -- or a generic constraint_name based on the table name
                                           -- this is only important when the table is an IOT
                                           -- GENERIC_CON is an expression constructed below
                                           -- it uses the join to ALL_CONSTRAINTS to see whether the proposed constraint name is available
                                       REGEXP_REPLACE( table_ddl,
                                                       '(constraint )("?)(\w+)("?)',
                                                       '\1' || CASE generic_con
                                                          WHEN 'Y'
                                                             THEN con_rename_adj
                                                          ELSE con_rename
                                                       END,
                                                       1,
                                                       0,
                                                       'i'
                                                     ),
                                       '(\.)("?)(' || :p_source_table || ')("?)',
                                       '\1' || UPPER( :p_table ),
                                       1,
                                       0,
                                       'i'
                                     ),
                       '(table)(\s+)("?)(' || :p_source_owner || ')("?)(\.)',
                       '\1\2' || UPPER( :p_owner ) || '\6',
                       1,
                       0,
                       'i'
                     ) table_ddl,
          
              -- this column was added for the REPLACE_TABLE procedure
          -- IN that procedure, after cloning the indexes, the table is renamed
          -- we have to rename the indexes back to their original names
          ' alter table '
       || UPPER( :p_source_owner || '.' || :p_source_table )
       || ' rename constraint '
       || CASE generic_con
             WHEN 'Y'
                THEN con_rename_adj
             ELSE con_rename
          END
       || ' to '
       || source_constraint rename_ddl,
          
          -- this column was added for the REPLACE_TABLE procedure
          -- IN that procedure, after cloning the indexes, the table is renamed
          -- we have to rename the indexes back to their original names
          'Constraint '
       || CASE generic_con
             WHEN 'Y'
                THEN con_rename_adj
             ELSE con_rename
          END
       || ' on table '
       || UPPER( :p_source_owner || '.' || :p_source_table )
       || ' renamed to '
       || source_constraint rename_msg,
       iot_type
  INTO :l_table_ddl,
       :l_rename_ddl,
       :l_rename_msg,
       :l_iot_type
  FROM ( SELECT
                -- this regular expression evaluates :p_TABLESPACE and modifies the DDL accordingly
                REGEXP_REPLACE
                          (
                            -- this regular expression evaluates :p_PARTITIONING paramater and removes partitioning information if necessary
                            REGEXP_REPLACE( table_ddl,
                                            CASE td_core.get_yn_ind( :p_partitioning )
                                               -- don't want partitioning
                                            WHEN 'no'
                                                  -- remove all partitioning
                                            THEN '(\(\s*partition.+\))\s*|(partition by).+\)\s*'
                                               ELSE NULL
                                            END,
                                            NULL,
                                            1,
                                            0,
                                            'in'
                                          ),
                            '(tablespace)(\s*)([^ ]+)([[:space:]]*)',
                            CASE
                               WHEN :p_tablespace IS NULL
                                  THEN '\1\2\3\4'
                               WHEN :p_tablespace = 'default'
                                  THEN NULL
                               ELSE '\1\2' || UPPER( :p_tablespace ) || '\4'
                            END,
                            1,
                            0,
                            'i'
                          ) table_ddl,
                con_rename, con_rename_adj, iot_type, source_owner, source_constraint, index_owner, index_name,
                
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
                        UPPER(    SUBSTR( :p_table, 1, 24 )
                               || '_'
                               || con_ext
                               || CASE constraint_type
                                     WHEN 'P'
                                        THEN NULL
                                     ELSE RANK( ) OVER( PARTITION BY con_ext ORDER BY source_constraint )
                                  END
                             
                             -- rank function gives us the constraint number by specific constraint extension (formulated below)
                             ) con_rename_adj,
                        iot_type, con_rename, table_ddl, constraint_name_confirm, source_owner, source_constraint,
                        index_owner, index_name
                  FROM ( SELECT DBMS_METADATA.get_ddl( 'TABLE', table_name, owner ) table_ddl, iot_type,
                                owner source_owner, constraint_name source_constraint, constraint_type, index_owner,
                                index_name,
                                
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
                               LEFT JOIN all_constraints USING( owner, table_name )
                         WHERE owner = UPPER( :p_source_owner )
                           AND table_name = UPPER( :p_source_table )
                           AND constraint_type = 'P' ) g1
                       LEFT JOIN
                       
                       -- joining here to see if the proposed constraint_name (con_rename) actually exists
                       ( SELECT owner constraint_owner_confirm, constraint_name constraint_name_confirm
                          FROM all_constraints ) g2
                       ON g1.con_rename = g2.constraint_name_confirm AND g2.constraint_owner_confirm = UPPER( :p_owner )
                       ))