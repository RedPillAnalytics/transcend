SET termout off
COLUMN CON_E_RENAME format a30

VAR p_table VARCHAR2(30)
VAR p_tablespace VARCHAR2(30)
VAR p_source_table VARCHAR2(30)
VAR p_source_owner VARCHAR2(30)
VAR p_constraint_regexp VARCHAR2(30)
VAR p_constraint_type VARCHAR2(30)
VAR p_seg_attributes VARCHAR2 (3)
VAR p_part_type VARCHAR2(30)
VAR p_index_type VARCHAR2(30)
VAR p_index_regexp VARCHAR2(30)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
VAR l_targ_part VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'whstage';
EXEC :p_table := 'customer_scd';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'customer_dim';
EXEC :p_constraint_type := NULL;
EXEC :p_seg_attributes := 'no';
EXEC :p_part_type := NULL;
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := NULL;
EXEC :p_handle_fkeys := 'yes';
EXEC :l_targ_part := 'NO';

SET termout on

SELECT CASE REPLACE
          WHEN 'Y'
             THEN REGEXP_REPLACE( index_ddl,
                                  '(\."?)(\w)+(")?( on)',
                                  '.' || idx_e_rename || ' \4',
                                  1,
                                  0,
                                  'i'
                                )
          ELSE index_ddl
       END index_ddl,
          ' alter index '
       || owner
       || '.'
       || CASE REPLACE
             WHEN 'Y'
                THEN idx_e_rename
             ELSE idx_rename
          END
       || ' rename to '
       || index_name rename_statment,
       object_name, idx_rename
  FROM ( SELECT
                -- IF idx_rename already exists (constructed below), then we will try to rename the index to something generic
                -- this name will only be used when an exception is raised
                -- this index is shown in debug mode
                UPPER(    :p_table
                       || '_'
                       || idx_e_ext
                       -- rank function gives us the index number by specific index extension (formulated below)
                       || RANK( ) OVER( PARTITION BY idx_e_ext ORDER BY index_name )
                     ) idx_e_rename,
                REGEXP_REPLACE
                       ( REGEXP_REPLACE( REGEXP_REPLACE( index_ddl,
                                                         '(alter index).+',
                                                         NULL,
                                                         1,
                                                         0,
                                                         'i'
                                                       ),
                                            '(\."?)('
                                         || UPPER( :p_source_table )
                                         || ')(\w*)("?)',
                                         '.' || UPPER( :p_table ) || '\3',
                                         1,
                                         0,
                                         'i'
                                       ),
                         '(")?(' || ind.owner || ')("?\.)',
                         UPPER( :p_owner ) || '.',
                         1,
                         0,
                         'i'
                       ) index_ddl,
                table_owner, table_name, ind.owner, index_name, idx_rename, partitioned,
                uniqueness, idx_e_ext, index_type,
                CASE
                   WHEN ao.object_name IS NULL
                      THEN 'N'
                   ELSE 'Y'
                END REPLACE, object_name
          FROM ( SELECT    REGEXP_REPLACE
                              
                              -- dbms_metadata pulls the metadata for the source object out of the dictionary
                           (    DBMS_METADATA.get_ddl( 'INDEX', index_name, owner ),
                                CASE
                                   -- target is not partitioned and no tablespace provided
                                WHEN :l_targ_part = 'NO' AND :p_tablespace IS NULL
                                      -- remove all partitioning and the local keyword
                                THEN '(\(\s*partition.+\))|local[[:space:]]*'
                                   -- target is not partitioned but tablespace is provided
                                WHEN :l_targ_part = 'NO' AND :p_tablespace IS NOT NULL
                                      -- strip out partitioned info and local keyword and tablespace clause
                                THEN '(\(\s*partition.+\))|local|(tablespace)\s*[^ ]+[[:space:]]*'
                                   -- target is partitioned and tablespace is provided
                                WHEN :l_targ_part = 'YES' AND :p_tablespace IS NOT NULL
                                      -- strip out partitioned info keeping local keyword and remove tablespace clause
                                THEN '(\(\s*partition.+\))|(tablespace)\s*[^ ]+[[:space:]]*'
                                   ELSE NULL
                                END,
                                NULL,
                                1,
                                0,
                                'in'
                              )
                        || CASE
                              -- IF tablespace is provided, tack it on the end
                           WHEN :p_tablespace IS NOT NULL
                                 THEN ' TABLESPACE ' || :p_tablespace
                              ELSE NULL
                           END index_ddl,
                        table_owner, table_name, owner, index_name,
                        
                        -- this is the index name that will be used in the first attempt
                        -- this index name is shown in debug mode
                        UPPER( REGEXP_REPLACE( index_name,
                                               '(")?' || :p_source_table || '(")?',
                                               :p_table,
                                               1,
                                               0,
                                               'i'
                                             )
                             ) idx_rename,
                        CASE
                           -- devise generic index extensions for the different types
                        WHEN index_type = 'BITMAP'
                              THEN 'BMI'
                           WHEN REGEXP_LIKE( index_type, '^function', 'i' )
                              THEN 'FNC'
                           WHEN uniqueness = 'UNIQUE'
                              THEN 'UK'
                           ELSE 'IK'
                        END idx_e_ext,
                        partitioned, uniqueness, index_type
                  FROM all_indexes ai
                 -- USE a CASE'd regular expression to determine whether to include global indexes
                WHERE  REGEXP_LIKE( partitioned,
                                    CASE
                                       WHEN REGEXP_LIKE( 'global', :p_part_type, 'i' )
                                          THEN 'NO'
                                       WHEN REGEXP_LIKE( 'local', :p_part_type, 'i' )
                                          THEN 'YES'
                                       ELSE '.'
                                    END,
                                    'i'
                                  )
                   AND table_name = UPPER( :p_source_table )
                   AND table_owner = UPPER( :p_source_owner )
                   -- USE an NVL'd regular expression to determine specific indexes to work on
                   AND REGEXP_LIKE( index_name, NVL( :p_index_regexp, '.' ), 'i' )
                   -- USE an NVL'd regular expression to determine the index types to worked on
                   AND REGEXP_LIKE( index_type, '^' || NVL( :p_index_type, '.' ), 'i' )
                   AND index_name NOT IN(
                          SELECT index_name
                            FROM all_tables dt JOIN all_constraints di
                                 USING( owner, table_name )
                           WHERE iot_type IS NOT NULL
                             AND constraint_type = 'P'
                             AND table_name <> UPPER( :p_source_table )
                             AND owner <> UPPER( :p_source_owner ))) ind
               LEFT JOIN
               all_objects ao
               ON ao.object_name = ind.idx_rename AND ao.owner = UPPER( :p_owner )
         WHERE subobject_name IS NULL )