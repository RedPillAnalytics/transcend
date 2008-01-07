SET termout off
COLUMN index_ddl format a130
COLUMN rename_ddl format a150
COLUMN idx_rename format a30
COLUMN idx_rename_adj format a30
SET echo off

EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SQLTERMINATOR',FALSE);
      -- we need the segment attributes so things go where we want them to
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SEGMENT_ATTRIBUTES',TRUE);
      -- don't want all the other storage aspects though
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE',FALSE );

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
VAR p_partname VARCHAR2(30)
VAR l_part_position number

EXEC :p_tablespace := null;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'whdata';
EXEC :p_table := 'td$customer_dim';
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'customer_dim';
EXEC :p_constraint_type := NULL;
EXEC :p_seg_attributes := 'no';
EXEC :p_part_type := NULL;
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := NULL;
EXEC :l_targ_part := 'YES';

SET termout on

          -- this case statement uses GENERIC_IDX column to determine the final index name
          -- if we are using a generic name, then perform the replace
SELECT UPPER( :p_owner ) index_owner, CASE generic_idx
          WHEN 'Y'
             THEN idx_rename_adj
          ELSE idx_rename
       END index_name, owner source_owner, index_name source_index, partitioned, uniqueness, index_type,
       CASE generic_idx
          WHEN 'Y'
             THEN REGEXP_REPLACE( index_ddl,
                                  '(\."?)(\w)+(")?( on)',
                                  '.' || idx_rename_adj || ' \4',
                                  1,
                                  0,
                                  'i'
                                )
          ELSE index_ddl
       END index_ddl,
          
          -- this column was added for the REPLACE_TABLE procedure
          -- in that procedure, after cloning the indexes, the table is renamed
          -- we have to rename the indexes back to their original names
          ' alter index '
       || owner
       || '.'
       || CASE generic_idx
             WHEN 'Y'
                THEN idx_rename_adj
             ELSE idx_rename
          END
       || ' rename to '
       || index_name rename_ddl,
          
          -- this column was added for the REPLACE_TABLE procedure
          -- in that procedure, after cloning the indexes, the table is renamed
          -- we have to rename the indexes back to their original names
          'Index '
       || owner
       || '.'
       || CASE generic_idx
             WHEN 'Y'
                THEN idx_rename_adj
             ELSE idx_rename
          END
       || ' renamed to '
       || index_name rename_msg
  FROM ( SELECT
                -- IF idx_rename already exists (constructed below), then we will try to rename the index to something generic
                -- this name will only be used when idx_rename name already exists
                UPPER(    SUBSTR( :p_table, 1, 24 )
                       || '_'
                       || idx_ext
                       -- rank function gives us the index number by specific index extension (formulated below)
                       || RANK( ) OVER( PARTITION BY idx_ext ORDER BY index_name )
                     ) idx_rename_adj,
                REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( index_ddl, '(alter index).+',
                                                                -- first remove any ALTER INDEX statements that may be included
                                                                -- this could occur if the indexes are in an unusable state, for instance
                                                                -- we don't care if they are unusable or not
                                                                NULL, 1, 0, 'i' ),
                                                '(\."?)(' || UPPER( :p_source_table ) || ')(\w*)("?)',
                                                '.' || UPPER( :p_table ) || '\3',
                                                -- replace source table name with target table
                                                1,
                                                0,
                                                'i'
                                              ),
                                '(")?(' || ind.owner || ')("?\.)',
                                UPPER( :p_owner ) || '.',
                                -- replace source owner with target owner
                                1,
                                0,
                                'i'
                              ) index_ddl,
                table_owner, table_name, ind.owner, index_name, idx_rename, partitioned, uniqueness, idx_ext,
                index_type,
                           -- this case expression determines whether to use the standard renamed index name
                           -- or whether to use the generic index name based on table name
                           -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                           -- if we match, then we need to use the generic index name
                CASE
                   WHEN( ao.object_name IS NULL AND LENGTH( idx_rename ) < 31 )
                      THEN 'N'
                   ELSE 'Y'
                END generic_idx, object_name
          FROM ( SELECT    REGEXP_REPLACE
                              
                              -- dbms_metadata pulls the metadata for the source object out of the dictionary
                           (    DBMS_METADATA.get_ddl( 'INDEX', index_name, owner ),
                                -- this CASE expression determines whether to strip partitioning information and tablespace information
                                -- tablespace desisions are based on the :p_TABLESPACE parameter
                                -- partitioning decisions are based on the structure of the target table
                                CASE
                                   -- target is not partitioned and neither :p_TABLESPACE or :p_PARTNAME are provided
                                WHEN :l_targ_part = 'NO' AND :p_tablespace IS NULL AND :p_partname IS NULL
                                      -- remove all partitioning and the local keyword
                                THEN '\s*(\(\s*partition.+\))|local\s*'
                                   -- target is not partitioned but :p_TABLESPACE or :p_PARTNAME is provided
                                WHEN :l_targ_part = 'NO' AND( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                      -- strip out partitioned info and local keyword and tablespace clause
                                THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                   -- target is partitioned and :p_TABLESPACE or :p_PARTNAME is provided
                                WHEN :l_targ_part = 'YES' AND( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                      -- strip out partitioned info keeping local keyword and remove tablespace clause
                                THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                   -- target is partitioned
                                   -- :p_TABLESPACE is null
                                   -- :p_PARTNAME is null
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
                              -- if 'default' is passed, then use the users default tablespace
                              -- a non-null value for :p_tablespace already stripped all tablespace information above
                              -- now just need to not put in the 'TABLESPACE' information here
                           WHEN LOWER( :p_tablespace ) = 'default'
                                 THEN NULL
                              -- if :p_TABLESPACE is provided, then previous tablespace information was stripped (above)
                              -- now we can just tack the new tablespace information on the end
                           WHEN :p_tablespace IS NOT NULL
                                 THEN ' TABLESPACE ' || UPPER( :p_tablespace )
                              WHEN :p_partname IS NOT NULL
                                 THEN    ' TABLESPACE '
                                      || NVL( ai.tablespace_name,
                                              ( SELECT tablespace_name
                                                 FROM all_ind_partitions
                                                WHERE index_name = ai.index_name
                                                  AND index_owner = ai.owner
                                                  AND partition_position = :l_part_position )
                                            )
                              ELSE NULL
                           END index_ddl,
                        table_owner, table_name, owner, index_name,
                        
                        -- this is the index name that will be used in the first attempt
                        -- basically, all cases of the previous table name are replaced with the new table name
                        UPPER( REGEXP_REPLACE( index_name, '(")?' || :p_source_table || '(")?', :p_table, 1, 0, 'i' )
                             ) idx_rename,
                        CASE
                           -- construct index extensions for the different index types
                           -- this will be used to construct generic index names
                           -- if the index name that we attempt to use first is already taken
                           -- then we'll construct generic index names based on table name and type
                        WHEN index_type = 'BITMAP'
                              THEN 'BI'
                           WHEN REGEXP_LIKE( index_type, '^function', 'i' )
                              THEN 'FI'
                           WHEN uniqueness = 'UNIQUE'
                              THEN 'UK'
                           ELSE 'IK'
                        END idx_ext,
                        partitioned, uniqueness, index_type
                  FROM all_indexes ai
                 -- USE an NVL'd regular expression to determine the partition types to worked on
                 -- when a regexp matching 'global' is passed, then do only global
                 -- when a regexp matching 'local' is passed, then do only local
                 -- when nothing is passed, it's a wildcard, so do all
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
                   -- will never want to try and build IOT indexes
                   -- we just eliminate them
                   AND index_type <> 'IOT - TOP'
                   -- USE an NVL'd regular expression to determine the specific indexes to work on
                   -- when nothing is passed for :p_INDEX_TYPE, then that is the same as passing a wildcard
                   AND REGEXP_LIKE( index_name, NVL( :p_index_regexp, '.' ), 'i' )
                   -- USE an NVL'd regular expression to determine the index types to worked on
                   -- when nothing is passed for :p_INDEX_TYPE, then that is the same as passing a wildcard
                   AND REGEXP_LIKE( index_type, '^' || NVL( :p_index_type, '.' ), 'i' )) ind
               LEFT JOIN
               all_objects ao ON ao.object_name = ind.idx_rename AND ao.owner = UPPER( :p_owner )
         WHERE subobject_name IS NULL )