SET termout off
COLUMN index_ddl format a130
COLUMN rename_ddl format a50
COLUMN rename_msg format a50
COLUMN drop_ddl format a50
COLUMN drop_msg format a50

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
VAR p_part_type VARCHAR2(30)
VAR p_index_type VARCHAR2(30)
VAR p_index_regexp VARCHAR2(30)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
VAR p_partname VARCHAR2(30)
VAR l_part_position number
VAR p_concurrent VARCHAR2(3)
VAR l_targ_part_flg VARCHAR2(3)
VAR l_src_part_flg VARCHAR2(3)
VAR default_tablespace VARCHAR2(30)

EXEC :p_tablespace := null;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'td_demo';
EXEC :p_table := 'stg$product_dim';
EXEC :p_source_owner := 'td_demo';
EXEC :p_source_table := 'product_dim';
EXEC :p_part_type := NULL;
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := NULL;
EXEC :p_concurrent := 'no'
EXEC :l_targ_part_flg := 'yes'
EXEC :l_src_part_flg := 'no'
EXEC :default_tablespace := '#*default_tablespace*#';

SET termout on

SELECT UPPER( :p_owner ) index_owner, new_index_name index_name, owner source_owner, index_name source_index,
                  partitioned, uniqueness, index_type,
                  REGEXP_REPLACE( index_ddl,
                                  '(\.)("?)(\w+)("?)(\s+)(on)',
                                  '\1' || new_index_name || '\5\6',
                                  1,
                                  0,
                                  'i'
                                ) index_ddl,
                  
                  -- this column was added for the REPLACE_TABLE procedure
                  -- in that procedure, after cloning the indexes, the table is renamed
                  -- we have to rename the indexes back to their original names
                  ' alter index ' || owner || '.' || new_index_name || ' rename to ' || index_name rename_ddl,
                  
                  -- this column was added for the REPLACE_TABLE procedure
                  -- in that procedure, after cloning the indexes, the table is renamed
                  -- we have to rename the indexes back to their original names
                  'Index ' || owner || '.' || new_index_name || ' renamed to ' || index_name rename_msg,

                  -- this column was added for the EXCHANGE_PARTITION procedure
                  -- this is to drop only the indexes that were added by the procedure
                  ' drop index ' || owner || '.' || new_index_name drop_ddl,
                  
                  -- this column was added for the EXCHANGE_PARTITION procedure
                  -- this is to drop only the indexes that were added by the procedure
                  'Index ' || owner || '.' || new_index_name || ' dropped' drop_msg
            FROM ( SELECT CASE
                             -- this case statement uses GENERIC_IDX plus other factors to determine what the new index name will be
                             -- IDX_RENAME is simply the current index name replacing any mentions of the source_table with target_table
                             --IDX_RENAME_ADJ is the index name using generic naming standards, such as <source_table>_<idx>2.
                             -- if both of these index names are already taken, then use a completely generic name, such as 'TD$_IDX' plus a timestamp.
                          WHEN idx_rename_adj = idx_rename AND generic_idx = 'Y'
                                THEN 'TD$_IDX' || TO_CHAR( SYSTIMESTAMP, 'mmddyyyyHHMISS' )
                             WHEN generic_idx = 'N'
                                THEN idx_rename
                             ELSE idx_rename_adj
                          END new_index_name,
                          owner, index_name, partitioned, uniqueness, index_type, index_ddl
                    FROM ( SELECT
                                  -- if idx_rename already exists (constructed below), then we will try to rename the index to something generic
                                  UPPER(    SUBSTR( :p_table, 1, 24 )
                                         || '_'
                                         || idx_ext
                                         -- rank function gives us the index number by specific index extension (formulated below)
                                         || RANK( ) OVER( PARTITION BY idx_ext ORDER BY index_name )
                                       ) idx_rename_adj,
                                  
                                  -- this regexp_replace replaces the current owner of the table with the new owner of the table
                                  REGEXP_REPLACE(
                                                  -- this regexp_replace will replace the source table with the target table
                                                  REGEXP_REPLACE( REGEXP_REPLACE( index_ddl,
                                                                                  '(alter index).+',
                                                                                  -- first remove any ALTER INDEX statements that may be included
                                                                                  -- this could occur if the indexes are in an unusable state, for instance
                                                                                  -- we don't care if they are unusable or not
                                                                                  NULL,
                                                                                  1,
                                                                                  0,
                                                                                  'i'
                                                                                ),
                                                                  '(\.)("?)(' || :p_source_table || ')("?)(\s+)(\()',
                                                                  '\1' || UPPER( :p_table ) || '\5\6',
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
                                  table_owner, table_name, ind.owner, index_name, idx_rename, partitioned, uniqueness,
                                  idx_ext, index_type,
                                  
                                  -- this case expression determines whether to use the standard renamed index name
                                  -- or whether to use the generic index name based on table name
                                  -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                                  -- if we match, then we need to use the generic index name
                                  CASE
                                     WHEN( index_name_confirm IS NULL AND LENGTH( idx_rename ) < 31 )
                                        THEN 'N'
                                     ELSE 'Y'
                                  END generic_idx
                            FROM ( SELECT    REGEXP_REPLACE
                                                
                                                -- dbms_metadata pulls the metadata for the source object out of the dictionary
                                             (    DBMS_METADATA.get_ddl( 'INDEX', index_name, owner ),
                                                  -- this CASE expression determines whether to strip partitioning information and tablespace information
                                                  -- tablespace desisions are based on the :p_TABLESPACE parameter
                                                  -- partitioning decisions are based on the structure of the target table
                                                  CASE
                                                     -- target is not partitioned and neither :p_TABLESPACE or :p_PARTNAME are provided
                                                  WHEN :l_targ_part_flg = 'no' AND :p_tablespace IS NULL
                                                       AND :p_partname IS NULL
                                                        -- remove all partitioning and the local keyword
                                                  THEN '\s*(\(\s*partition.+\))|local\s*'
                                                     -- target is not partitioned but :p_TABLESPACE or :p_PARTNAME is provided
                                                  WHEN :l_targ_part_flg = 'no'
                                                  AND ( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                                        -- strip out partitioned info and local keyword and tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned and :p_TABLESPACE or :p_PARTNAME is provided
                                                  WHEN :l_targ_part_flg = 'yes'
                                                  AND ( :p_tablespace IS NOT NULL OR :p_partname IS NOT NULL )
                                                        -- strip out partitioned info keeping local keyword and remove tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned
                                                     -- :p_TABLESPACE is null
                                                     -- :p_PARTNAME is null
                                                  WHEN :l_targ_part_flg = 'yes' AND :p_tablespace IS NULL
                                                       AND :p_partname IS NULL
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
                                                -- if constant default_tablespace is passed, then use the users default tablespace
                                                -- a non-null value for :p_tablespace already stripped all tablespace information above
                                                -- now just need to not put in the 'TABLESPACE' information here
                                             WHEN LOWER( :p_tablespace ) = :default_tablespace
                                                   THEN NULL
                                                -- if :p_TABLESPACE is provided, then previous tablespace information was stripped (above)
                                                -- now we can just tack the new tablespace information on the end
                                             WHEN :p_tablespace IS NOT NULL
                                                   THEN ' TABLESPACE ' || UPPER( :p_tablespace )
                                                WHEN :p_partname IS NOT NULL
                                                   THEN    ' TABLESPACE '
                                                        || NVL( ai.tablespace_name,
                                                                ( SELECT tablespace_name
                                                                    FROM ( SELECT index_name,
                                                                                  CASE WHEN subpartition_name IS NULL THEN ip.tablespace_name ELSE isp.tablespace_name END tablespace_name,
                                                                                  CASE WHEN subpartition_name IS NULL THEN partition_position ELSE subpartition_position END partition_position,
                                                                                  index_owner,
                                                                                  CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                                                                                  CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                                                                             FROM  all_ind_partitions ip
                                                                             left JOIN all_ind_subpartitions isp
                                                                                  USING (index_owner, index_name, partition_name ))
                                                                   WHERE index_name = ai.index_name
                                                                     AND index_owner = ai.owner
                                                                     AND partition_position = :l_part_position )
                                                              )
                                                ELSE NULL
                                             END 
--                                          || CASE WHEN td_core.get_yn_ind( :l_targ_part_flg ) = 'yes' AND td_core.get_yn_ind( :l_src_part_flg ) = 'no' THEN ' LOCAL' ELSE NULL END
                                          || ' LOCAL' index_ddl,
                                          table_owner, table_name, owner, index_name,
                                          
                                          -- this is the index name that will be used in the first attempt
                                          -- basically, all cases of the previous table name are replaced with the new table name
                                          UPPER( REGEXP_REPLACE( index_name,
                                                                 '(")?' || :p_source_table || '(")?',
                                                                 :p_table,
                                                                 1,
                                                                 0,
                                                                 'i'
                                                               )
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
                                     -- iot indexes provide a problem when in CONCURRENT mode
                                     -- the code just handles the errors with exceptions
                                     -- but CONCURRENT processes are subject to exceptions in the flow of the program
                                     -- so we just don't support certain paradigms in concurrent mode
                                     -- one of them is building having a mismatch between table types when considering IOT's
                                     AND index_type <>
                                                  CASE td_core.get_yn_ind( :p_concurrent )
                                                     WHEN 'yes'
                                                        THEN 'IOT - TOP'
                                                     ELSE '~'
                                                  END
                                     -- USE an NVL'd regular expression to determine the specific indexes to work on
                                     -- when nothing is passed for :p_INDEX_TYPE, then that is the same as passing a wildcard
                                     AND REGEXP_LIKE( index_name, NVL( :p_index_regexp, '.' ), 'i' )
                                     -- USE an NVL'd regular expression to determine the index types to worked on
                                     -- when nothing is passed for :p_INDEX_TYPE, then that is the same as passing a wildcard
                                     AND REGEXP_LIKE( index_type, '^' || NVL( :p_index_type, '.' ), 'i' )) ind
                                 LEFT JOIN
                                 ( SELECT index_name index_name_confirm, owner index_owner_confirm
                                    FROM all_indexes ) aii
                                 ON aii.index_name_confirm = ind.idx_rename
                               AND aii.index_owner_confirm = UPPER( :p_owner )
                                 ))