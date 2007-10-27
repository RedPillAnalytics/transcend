CREATE OR REPLACE PACKAGE BODY td_ddl
AS
   -- find records in p_source_table that match the values of the partitioned column in p_table
   -- This procedure uses an undocumented database function called tbl$or$idx$part$num.
   -- There are two "magic" numbers that are required to make it work correctly.
   -- The defaults will quite often work.
   -- The simpliest way to find which magic numbers make this function work is to
   -- do a partition exchange on the target table and trace that statement.
   PROCEDURE populate_partname(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 0,
      p_p_num           NUMBER DEFAULT 65535
   )
   AS
      l_dsql            LONG;
      l_num_msg         VARCHAR2( 100 )
                                   := 'Number of records inserted into TD_PART_GTT table';
      -- to catch empty cursors
      l_source_column   all_part_key_columns.column_name%TYPE;
      l_results         NUMBER;
      o_ev              evolve_ot               := evolve_ot( p_module      => 'populate_partname' );
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_high_value      all_tab_partitions.high_value%TYPE;
   BEGIN
      td_sql.check_table( p_owner            => p_owner,
                          p_table            => p_table,
                          p_partname         => p_partname,
                          p_partitioned      => 'yes'
                        );

      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position, high_value
           INTO l_part_position, l_high_value
           FROM all_tab_partitions
          WHERE table_owner = UPPER( p_owner )
            AND table_name = UPPER( p_table )
            AND partition_name = UPPER( p_partname );

         INSERT INTO td_part_gtt
                     ( table_owner, table_name, partition_name,
                       partition_position
                     )
              VALUES ( UPPER( p_owner ), UPPER( p_table ), UPPER( p_partname ),
                       l_part_position
                     );

         td_inst.log_cnt_msg( SQL%ROWCOUNT, l_num_msg, 4 );
      ELSE
         IF p_source_column IS NULL
         THEN
            SELECT column_name
              INTO l_source_column
              FROM all_part_key_columns
             WHERE NAME = UPPER( p_table ) AND owner = UPPER( p_owner );
         ELSE
            l_source_column := p_source_column;
         END IF;

         o_ev.change_action( 'insert into td_part_gtt' );
         l_results :=
            td_sql.exec_sql
               ( p_sql      =>    'insert into td_part_gtt (table_owner, table_name, partition_name, partition_position) '
                               || ' SELECT table_owner, table_name, partition_name, partition_position'
                               || '  FROM all_tab_partitions'
                               || ' WHERE table_owner = '''
                               || UPPER( p_owner )
                               || ''' AND table_name = '''
                               || UPPER( p_table )
                               || ''' AND partition_position IN '
                               || ' (SELECT DISTINCT tbl$or$idx$part$num("'
                               || UPPER( p_owner )
                               || '"."'
                               || UPPER( p_table )
                               || '", 0, '
                               || p_d_num
                               || ', '
                               || p_p_num
                               || ', "'
                               || UPPER( l_source_column )
                               || '")	 FROM '
                               || UPPER( p_source_owner )
                               || '.'
                               || UPPER( p_source_object )
                               || ') '
                               || 'ORDER By partition_position'
               );
         td_inst.log_cnt_msg( l_results, l_num_msg, 4 );
      END IF;

      o_ev.clear_app_info;
   END populate_partname;

   PROCEDURE truncate_table(
      p_owner   VARCHAR2,
      p_table   VARCHAR2,
      p_reuse   VARCHAR2 DEFAULT 'no'
   )
   IS
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot         := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      -- confirm that the table exists
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      td_sql.exec_sql( p_sql       =>    'truncate table '
                                      || p_owner
                                      || '.'
                                      || p_table
                                      || CASE
                                            WHEN td_ext.is_true( p_reuse )
                                               THEN ' reuse storage'
                                            ELSE NULL
                                         END,
                       p_auto      => 'yes'
                     );
      td_inst.log_msg( l_tab_name || ' truncated' );
      o_ev.clear_app_info;
   END truncate_table;

   -- drop a table
   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2
            DEFAULT 'yes' )
   IS
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot         := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      -- confirm that the table exists
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      td_sql.exec_sql( p_sql       =>    'drop table '
                                      || p_owner
                                      || '.'
                                      || p_table
                                      || CASE
                                            WHEN td_ext.is_true( p_purge )
                                               THEN ' purge'
                                            ELSE NULL
                                         END,
                       p_auto      => 'yes'
                     );
      td_inst.log_msg( l_tab_name || ' dropped' );
      o_ev.clear_app_info;
   END drop_table;

   -- builds a new table based on a current one
   PROCEDURE build_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_partitioning   VARCHAR2 DEFAULT 'yes',
      p_rows           VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'ignore'
   )
   IS
      l_ddl            LONG;
      l_idx_cnt        NUMBER                        := 0;
      l_tab_name       VARCHAR2( 61 )               := UPPER( p_owner || '.' || p_table );
      l_src_name       VARCHAR2( 61 ) := UPPER( p_source_owner || '.' || p_source_table );
      l_part_type      VARCHAR2( 6 );
      l_targ_part      all_tables.partitioned%TYPE;
      l_rows           BOOLEAN                       := FALSE;
      e_dup_idx_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_idx_name, -955 );
      e_dup_col_list   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_col_list, -1408 );
      o_ev             evolve_ot                      := evolve_ot( p_module      => 'build_table' );
   BEGIN
      -- confirm that the source table
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_source_owner, p_table => p_source_table );
      -- don't want any constraints pulled
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'CONSTRAINTS',
                                         FALSE
                                       );
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'REF_CONSTRAINTS',
                                         FALSE
                                       );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SQLTERMINATOR',
                                         FALSE
                                       );
      -- we need the segment attributes so things go where we want them to
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SEGMENT_ATTRIBUTES',
                                         TRUE
                                       );
      -- don't want all the other storage aspects though
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE',
                                         FALSE );
      o_ev.change_action( 'Extract DDL' );

      -- select DDL into a variable
      SELECT REGEXP_REPLACE
                ( REGEXP_REPLACE
                     ( REGEXP_REPLACE
                          ( DBMS_METADATA.get_ddl( 'TABLE', table_name, owner ),
                            CASE
                               -- don't want partitioning
                            WHEN td_ext.get_yn_ind( p_partitioning ) = 'no'
                                  -- remove all partitioning
                            THEN '(\(\s*partition.+\))\s*|(partition by).+\)\s*'
                               ELSE NULL
                            END,
                            NULL,
                            1,
                            0,
                            'in'
                          ),
                       '(\."?)(' || p_source_table || ')(\w*)("?)',
                       '.' || p_table || '\3',
                       1,
                       0,
                       'i'
                     ),
                  '(")?(' || p_source_owner || ')("?\.)',
                  p_owner || '.',
                  1,
                  0,
                  'i'
                ) table_ddl
        INTO l_ddl
        FROM all_tables
       WHERE owner = UPPER( p_source_owner ) AND table_name = UPPER( p_source_table );

      -- if a tablespace is provided then replace that
      IF p_tablespace IS NOT NULL
      THEN
         l_ddl :=
            REGEXP_REPLACE( l_ddl,
                            '(tablespace)(\s*)([^ ]+)([[:space:]]*)',
                            '\1\2' || p_tablespace || '\4',
                            1,
                            0,
                            'i'
                          );
      END IF;

      td_sql.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
      td_inst.log_msg( 'Table ' || l_tab_name || ' created' );

      -- if you want the records as well
      IF td_ext.is_true( p_rows )
      THEN
         insert_table( p_source_owner       => p_source_owner,
                       p_source_object      => p_source_table,
                       p_owner              => p_owner,
                       p_table              => p_table
                     );
      END IF;

      -- we want to gather statistics
      -- we gather statistics first before the indexes are built
      -- the indexes will collect there own statistics when they are built
      -- that is why we don't cascade
      CASE
         WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
         THEN
            update_stats( p_owner => p_owner, p_table => p_table );
         -- we want to transfer the statistics from the current segment into the new segment
         -- this is preferable if automatic stats are handling stats collection
         -- and you want the load time not to suffer from statistics gathering
      WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
         THEN
            update_stats( p_owner             => p_owner,
                          p_table             => p_table,
                          p_source_owner      => p_owner,
                          p_source_table      => p_source_table
                        );
         -- do nothing with stats
         -- this is preferable if stats are gathered on the staging segment prior to being exchanged in
         -- OWB can do this, for example
      WHEN REGEXP_LIKE( 'ignore', p_statistics, 'i' )
         THEN
            NULL;
         ELSE
            raise_application_error( td_ext.get_err_cd( 'unrecognized_parm' ),
                                        td_ext.get_err_msg( 'unrecognized_parm' )
                                     || ' : '
                                     || p_statistics
                                   );
      END CASE;

      o_ev.clear_app_info;
   END build_table;

   -- builds the indexes from one table on another
   -- if both the source and target are partitioned tables, then the index DDL is left alone
   -- if the source is partitioned and the target is not, then all local indexes are created as non-local
   -- if P_TABLESPACE is provided, then that tablespace name is used, regardless of the DDL that is pulled
   PROCEDURE build_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_partname       VARCHAR2 DEFAULT NULL
   )
   IS
      l_ddl             LONG;
      l_idx_cnt         NUMBER                                       := 0;
      l_tab_name        VARCHAR2( 61 )              := UPPER( p_owner || '.' || p_table );
      l_src_name        VARCHAR2( 61 )
                                      := UPPER( p_source_owner || '.' || p_source_table );
      l_part_type       VARCHAR2( 6 );
      l_targ_part       all_tables.partitioned%TYPE;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_rows            BOOLEAN                                      := FALSE;
      e_dup_idx_name    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_idx_name, -955 );
      e_dup_col_list    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_col_list, -1408 );
      o_ev              evolve_ot                   := evolve_ot( p_module      => 'build_indexes' );
   BEGIN
      -- confirm that parameters are compatible
      -- go ahead and write a CASE statement so adding more later is easier
      CASE
         WHEN p_tablespace IS NOT NULL AND p_partname IS NOT NULL
         THEN
            raise_application_error( td_inst.get_err_cd( 'parms_not_compatible' ),
                                        td_inst.get_err_msg( 'parms_not_compatible' )
                                     || ': P_TABLESPACE and P_PARTNAME'
                                   );
         ELSE
            NULL;
      END CASE;

      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_sql.check_table( p_owner         => p_source_owner,
                          p_table         => p_source_table,
                          p_partname      => p_partname
                        );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SQLTERMINATOR',
                                         FALSE
                                       );
      -- we need the segment attributes so things go where we want them to
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SEGMENT_ATTRIBUTES',
                                         TRUE
                                       );
      -- don't want all the other storage aspects though
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE',
                                         FALSE );
      o_ev.change_action( 'Build indexes' );

      -- find out if the target table is partitioned so we know how to formulate the index ddl
      SELECT partitioned
        INTO l_targ_part
        FROM all_tables
       WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner );

      -- if P_PARTNAME is specified, then I need the partition position is required
      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position
           INTO l_part_position
           FROM all_tab_partitions
          WHERE table_name = UPPER( p_source_table )
            AND table_owner = UPPER( p_source_owner )
            AND partition_name = UPPER( p_partname );
      END IF;

      -- create a cursor containing the DDL from the target indexes
      FOR c_indexes IN
         (
          -- this case statement uses GENERIC_IDX column to determine the final index name
          -- if we are using a generic name, then perform the replace
          SELECT UPPER( p_owner ) index_owner,
                 CASE generic_idx
                    WHEN 'Y'
                       THEN idx_rename_adj
                    ELSE idx_rename
                 END index_name, owner source_owner, index_name source_index, partitioned,
                 uniqueness, index_type,
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
                          UPPER
                             (    SUBSTR( p_table, 1, 24 )
                               || '_'
                               || idx_ext
                               -- rank function gives us the index number by specific index extension (formulated below)
                               || RANK( ) OVER( PARTITION BY idx_ext ORDER BY index_name )
                             ) idx_rename_adj,
                          REGEXP_REPLACE
                             ( REGEXP_REPLACE( REGEXP_REPLACE( index_ddl,
                                                               '(alter index).+',
                                                               -- first remove any ALTER INDEX statements that may be included
                                                               -- this could occur if the indexes are in an unusable state, for instance
                                                               -- we don't care if they are unusable or not
                                                               NULL,
                                                               1,
                                                               0,
                                                               'i'
                                                             ),
                                                  '(\."?)('
                                               || UPPER( p_source_table )
                                               || ')(\w*)("?)',
                                               '.' || UPPER( p_table ) || '\3',
                                               -- replace source table name with target table
                                               1,
                                               0,
                                               'i'
                                             ),
                               '(")?(' || ind.owner || ')("?\.)',
                               UPPER( p_owner ) || '.',
                               -- replace source owner with target owner
                               1,
                               0,
                               'i'
                             ) index_ddl,
                          table_owner, table_name, ind.owner, index_name, idx_rename,
                          partitioned, uniqueness, idx_ext, index_type,
                          
                          -- this case expression determines whether to use the standard renamed index name
                          -- or whether to use the generic index name based on table name
                          -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                          -- if we match, then we need to use the generic index name
                          CASE
                             WHEN( ao.object_name IS NULL AND LENGTH( idx_rename ) < 31
                                 )
                                THEN 'N'
                             ELSE 'Y'
                          END generic_idx,
                          object_name
                    FROM ( SELECT    REGEXP_REPLACE
                                        
                                        -- dbms_metadata pulls the metadata for the source object out of the dictionary
                                     (    DBMS_METADATA.get_ddl( 'INDEX',
                                                                 index_name,
                                                                 owner
                                                               ),
                                          -- this CASE expression determines whether to strip partitioning information and tablespace information
                                          -- tablespace desisions are based on the P_TABLESPACE parameter
                                          -- partitioning decisions are based on the structure of the target table
                                          CASE
                                             -- target is not partitioned and neither P_TABLESPACE or P_PARTNAME are provided
                                          WHEN l_targ_part = 'NO'
                                          AND p_tablespace IS NULL
                                          AND p_partname IS NULL
                                                -- remove all partitioning and the local keyword
                                          THEN '\s*(\(\s*partition.+\))|local\s*'
                                             -- target is not partitioned but P_TABLESPACE or P_PARTNAME is provided
                                          WHEN l_targ_part = 'NO'
                                          AND (    p_tablespace IS NOT NULL
                                                OR p_partname IS NOT NULL
                                              )
                                                -- strip out partitioned info and local keyword and tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                             -- target is partitioned and P_TABLESPACE or P_PARTNAME is provided
                                          WHEN l_targ_part = 'YES'
                                          AND (    p_tablespace IS NOT NULL
                                                OR p_partname IS NOT NULL
                                              )
                                                -- strip out partitioned info keeping local keyword and remove tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                             -- target is partitioned
                                             -- P_TABLESPACE is null
                                             -- P_PARTNAME is null
                                          WHEN l_targ_part = 'YES'
                                          AND p_tablespace IS NULL
                                          AND p_partname IS NULL
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
                                        -- a non-null value for p_tablespace already stripped all tablespace information above
                                        -- now just need to not put in the 'TABLESPACE' information here
                                     WHEN LOWER( p_tablespace ) = 'default'
                                           THEN NULL
                                        -- if P_TABLESPACE is provided, then previous tablespace information was stripped (above)
                                        -- now we can just tack the new tablespace information on the end
                                     WHEN p_tablespace IS NOT NULL
                                           THEN ' TABLESPACE ' || UPPER( p_tablespace )
                                        WHEN p_partname IS NOT NULL
                                           THEN    ' TABLESPACE '
                                                || NVL( ai.tablespace_name,
                                                        ( SELECT tablespace_name
                                                           FROM all_ind_partitions
                                                          WHERE index_name = ai.index_name
                                                            AND index_owner = ai.owner
                                                            AND partition_position =
                                                                           l_part_position )
                                                      )
                                        ELSE NULL
                                     END index_ddl,
                                  table_owner, table_name, owner, index_name,
                                  
                                  -- this is the index name that will be used in the first attempt
                                  -- basically, all cases of the previous table name are replaced with the new table name
                                  UPPER( REGEXP_REPLACE( index_name,
                                                         '(")?' || p_source_table
                                                         || '(")?',
                                                         p_table,
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
                                     WHEN REGEXP_LIKE( index_type,
                                                       '^function',
                                                       'i'
                                                     )
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
                                                 WHEN REGEXP_LIKE( 'global',
                                                                   p_part_type,
                                                                   'i'
                                                                 )
                                                    THEN 'NO'
                                                 WHEN REGEXP_LIKE( 'local',
                                                                   p_part_type,
                                                                   'i'
                                                                 )
                                                    THEN 'YES'
                                                 ELSE '.'
                                              END,
                                              'i'
                                            )
                             AND table_name = UPPER( p_source_table )
                             AND table_owner = UPPER( p_source_owner )
                             -- USE an NVL'd regular expression to determine the specific indexes to work on
                             -- when nothing is passed for P_INDEX_TYPE, then that is the same as passing a wildcard
                             AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
                             -- USE an NVL'd regular expression to determine the index types to worked on
                             -- when nothing is passed for P_INDEX_TYPE, then that is the same as passing a wildcard
                             AND REGEXP_LIKE( index_type,
                                              '^' || NVL( p_index_type, '.' ),
                                              'i'
                                            )) ind
                         LEFT JOIN
                         all_objects ao
                         ON ao.object_name = ind.idx_rename
                            AND ao.owner = UPPER( p_owner )
                   WHERE subobject_name IS NULL ))
      LOOP
         l_rows := TRUE;
         o_ev.change_action( 'Format index DDL' );
         o_ev.change_action( 'Execute index DDL' );

         BEGIN
            td_sql.exec_sql( p_sql => c_indexes.index_ddl, p_auto => 'yes' );
            td_inst.log_msg( 'Index ' || c_indexes.index_name || ' built', 3 );
            l_idx_cnt := l_idx_cnt + 1;
            o_ev.change_action( 'insert into td_build_idx_gtt' );

            INSERT INTO td_build_idx_gtt
                        ( index_owner, index_name,
                          src_index_owner, src_index_name,
                          create_ddl,
                          rename_ddl,
                          rename_msg
                        )
                 VALUES ( c_indexes.index_owner, c_indexes.index_name,
                          c_indexes.source_owner, c_indexes.source_index,
                          SUBSTR( c_indexes.index_ddl, 1, 3998 ) || '>>',
                          c_indexes.rename_ddl,
                          SUBSTR( c_indexes.rename_msg, 1, 3998 ) || '>>'
                        );
         EXCEPTION
            -- if a duplicate column list of indexes already exist, log it, but continue
            WHEN e_dup_col_list
            THEN
               td_inst.log_msg(    'Index comparable to '
                                || c_indexes.source_index
                                || ' already exists',
                                3
                              );
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No matching indexes found on ' || l_src_name );
      ELSE
         td_inst.log_msg(    l_idx_cnt
                          || ' index'
                          || CASE
                                WHEN l_idx_cnt = 1
                                   THEN NULL
                                ELSE 'es'
                             END
                          || ' built on '
                          || l_tab_name
                        );
      END IF;

      o_ev.clear_app_info;
   END build_indexes;

   -- renames cloned indexes on a particular table back to their original names
   PROCEDURE rename_indexes
   IS
      l_idx_cnt   NUMBER  := 0;
      l_rows      BOOLEAN := FALSE;
      o_ev        evolve_ot  := evolve_ot( p_module => 'rename_indexes' );
   BEGIN
      FOR c_idxs IN ( SELECT *
                       FROM td_build_idx_gtt )
      LOOP
         BEGIN
            l_rows := TRUE;
            td_sql.exec_sql( p_sql => c_idxs.rename_ddl, p_auto => 'yes' );
            td_inst.log_msg( c_idxs.rename_msg, 3 );
            l_idx_cnt := l_idx_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No previously cloned indexes identified' );
      ELSE
         td_inst.log_msg(    l_idx_cnt
                          || ' index'
                          || CASE
                                WHEN l_idx_cnt = 1
                                   THEN NULL
                                ELSE 'es'
                             END
                          || ' renamed'
                        );
      END IF;

      -- commit is required to clear out the contents of the global temporary table
      COMMIT;
      o_ev.clear_app_info;
   END rename_indexes;

   -- builds the constraints from one table on another
   PROCEDURE build_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_seg_attributes      VARCHAR2 DEFAULT 'no',
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL
   )
   IS
      l_targ_part       all_tables.partitioned%TYPE;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_con_cnt         NUMBER                                       := 0;
      l_tab_name        VARCHAR2( 61 )              := UPPER( p_owner || '.' || p_table );
      l_src_name        VARCHAR2( 61 )
                                      := UPPER( p_source_owner || '.' || p_source_table );
      l_rows            BOOLEAN                                      := FALSE;
      l_retry_ddl       BOOLEAN                                      := FALSE;
      e_dup_con_name    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_con_name, -2264 );
      e_dup_not_null    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_not_null, -1442 );
      e_dup_pk          EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_pk, -2260 );
      e_dup_fk          EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_fk, -2275 );
      o_ev              evolve_ot               := evolve_ot( p_module      => 'build_constraints' );
   BEGIN
      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_sql.check_table( p_owner         => p_source_owner,
                          p_table         => p_source_table,
                          p_partname      => p_partname
                        );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SQLTERMINATOR',
                                         FALSE
                                       );
      -- determine whether information about segments is included
      -- for unique and primary key constraints, these are linked to an index
      -- with segment_attributes true, the USING INDEX and other information will be included
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SEGMENT_ATTRIBUTES',
                                         CASE LOWER( p_seg_attributes )
                                            WHEN 'yes'
                                               THEN TRUE
                                            ELSE FALSE
                                         END
                                       );

      -- need this to determine how to build constraints associated with indexes on target table
      SELECT partitioned
        INTO l_targ_part
        FROM all_tables
       WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner );

      -- if P_PARTNAME is specified, then I need the partition position is required
      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position
           INTO l_part_position
           FROM all_tab_partitions
          WHERE table_name = UPPER( p_source_table )
            AND table_owner = UPPER( p_source_owner )
            AND partition_name = UPPER( p_partname );
      END IF;

      o_ev.change_action( 'Build constraints' );

      FOR c_constraints IN
         ( SELECT UPPER( p_owner ) constraint_owner,
                  CASE generic_con
                     WHEN 'Y'
                        THEN con_rename_adj
                     ELSE con_rename
                  END constraint_name,
                  owner source_owner, table_name, constraint_name source_constraint,
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
                  || constraint_name rename_msg
            FROM ( SELECT
                          -- IF con_rename already exists (constructed below), then we will try to rename the constraint to something generic
                          -- this name will only be used when con_rename name already exists
                          UPPER
                             (    SUBSTR( p_table, 1, 24 )
                               || '_'
                               || con_ext
                               -- rank function gives us the constraint number by specific constraint extension (formulated below)
                               || RANK( ) OVER( PARTITION BY con_ext ORDER BY constraint_name )
                             ) con_rename_adj,
                          REGEXP_REPLACE
                             ( REGEXP_REPLACE( REGEXP_REPLACE( constraint_ddl,
                                                               '(alter constraint).+',
                                                               NULL,
                                                               1,
                                                               0,
                                                               'i'
                                                             ),
                                                  '(\.|constraint +)("?)('
                                               || UPPER( p_source_table )
                                               || ')(\w*)("?)',
                                               '\1' || UPPER( p_table ) || '\4',
                                               1,
                                               0,
                                               'i'
                                             ),
                               '(")?(' || con.owner || ')("?\.)',
                               UPPER( p_owner ) || '.',
                               1,
                               0,
                               'i'
                             ) constraint_ddl,
                          con.owner, table_name, constraint_name, con_rename, index_owner,
                          index_name, con_ext, constraint_type,
                          
                          -- this case expression determines whether to use the standard renamed constraint name
                          -- OR whether to use the generic constraint name based on table name
                          -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                          -- IF we match, then we need to use the generic constraint name
                          CASE
                             WHEN( ao.object_name IS NULL AND LENGTH( con_rename ) < 31
                                 )
                                THEN 'N'
                             ELSE 'Y'
                          END generic_con,
                          object_name
                    FROM ( SELECT    REGEXP_REPLACE
                                        
                                        -- dbms_metadata pulls the metadata for the source object out of the dictionary
                                     (    DBMS_METADATA.get_ddl
                                                         ( CASE constraint_type
                                                              WHEN 'R'
                                                                 THEN 'REF_CONSTRAINT'
                                                              ELSE 'CONSTRAINT'
                                                           END,
                                                           constraint_name,
                                                           ac.owner
                                                         ),
                                          -- this CASE expression determines whether to strip partitioning information and tablespace information
                                          -- TABLESPACE desisions are based on the P_TABLESPACE parameter
                                          -- partitioning decisions are based on the structure of the target table
                                          CASE
                                             -- target is not partitioned and neither p_TABLESPACE or p_PARTNAME are provided
                                          WHEN l_targ_part = 'NO'
                                          AND p_tablespace IS NULL
                                          AND p_partname IS NULL
                                                -- remove all partitioning and the local keyword
                                          THEN '\s*(\(\s*partition.+\))|local\s*'
                                             -- target is not partitioned but p_TABLESPACE or p_PARTNAME is provided
                                          WHEN l_targ_part = 'NO'
                                          AND (    p_tablespace IS NOT NULL
                                                OR p_partname IS NOT NULL
                                              )
                                                -- strip out partitioned info and local keyword and tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                             -- target is partitioned and p_TABLESPACE or p_PARTNAME is provided
                                          WHEN l_targ_part = 'YES'
                                          AND (    p_tablespace IS NOT NULL
                                                OR p_partname IS NOT NULL
                                              )
                                                -- strip out partitioned info keeping local keyword and remove tablespace clause
                                          THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                             -- target is partitioned
                                             -- p_tablespace IS NULL
                                             -- p_partname IS NULL
                                          WHEN l_targ_part = 'YES'
                                          AND p_tablespace IS NULL
                                          AND p_partname IS NULL
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
                                        -- a non-null value for p_tablespace already stripped all tablespace information above
                                        -- now just need to not put in the 'TABLESPACE' information here
                                     WHEN LOWER( p_tablespace ) = 'default'
                                           THEN NULL
                                        -- IF p_TABLESPACE is provided, then previous tablespace information was stripped (above)
                                        -- now we can just tack the new tablespace information on the end
                                     WHEN p_tablespace IS NOT NULL
                                           THEN ' TABLESPACE ' || UPPER( p_tablespace )
                                        WHEN p_partname IS NOT NULL
                                           THEN    ' TABLESPACE '
                                                || NVL( ai.tablespace_name,
                                                        ( SELECT tablespace_name
                                                           FROM all_ind_partitions
                                                          WHERE index_name = ac.index_name
                                                            AND index_owner = ac.owner
                                                            AND partition_position =
                                                                           l_part_position )
                                                      )
                                        ELSE NULL
                                     END constraint_ddl,
                                  ac.owner, ac.table_name, ac.constraint_name,
                                  ac.index_owner, ac.index_name,
                                  
                                  -- this is the constraint name that will be used if it doesn't already exist
                                  -- basically, all cases of the previous table name are replaced with the new table name
                                  UPPER( REGEXP_REPLACE( constraint_name,
                                                         '(")?' || p_source_table
                                                         || '(")?',
                                                         p_table,
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
                                  END con_ext,
                                  constraint_type
                            FROM all_constraints ac LEFT JOIN all_indexes ai
                                 ON ac.index_owner = ai.owner
                               AND ac.index_name = ai.index_name
                           WHERE ac.table_name = UPPER( p_source_table )
                             AND ac.owner = UPPER( p_source_owner )
                             AND REGEXP_LIKE( constraint_name,
                                              NVL( p_constraint_regexp, '.' ),
                                              'i'
                                            )
                             AND REGEXP_LIKE( constraint_type,
                                              NVL( p_constraint_type, '.' ),
                                              'i'
                                            )) con
                         LEFT JOIN
                         all_objects ao
                         ON ao.object_name = con.con_rename
                       AND ao.owner = UPPER( p_owner )
                       AND object_type = 'INDEX'
                         ))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;

         BEGIN
            td_sql.exec_sql( p_sql => c_constraints.constraint_ddl, p_auto => 'yes' );
            td_inst.log_msg( 'Constraint ' || c_constraints.constraint_name || ' built',
                             3
                           );
            l_con_cnt := l_con_cnt + 1;
            o_ev.change_action( 'insert into td_build_idx_gtt' );

            INSERT INTO td_build_con_gtt
                        ( table_owner, table_name,
                          constraint_name, src_constraint_name,
                          index_name, index_owner,
                          create_ddl,
                          create_msg, rename_ddl,
                          rename_msg
                        )
                 VALUES ( c_constraints.source_owner, c_constraints.table_name,
                          c_constraints.constraint_name, c_constraints.source_constraint,
                          c_constraints.index_name, c_constraints.index_owner,
                          SUBSTR( c_constraints.constraint_ddl, 1, 3998 ) || '>>',
                          c_constraints.rename_ddl, NULL,
                          SUBSTR( c_constraints.rename_msg, 1, 3998 ) || '>>'
                        );
         EXCEPTION
            WHEN e_dup_pk
            THEN
               td_inst.log_msg(    'Primary key constraint already exists on table '
                                || l_tab_name,
                                3
                              );
            WHEN e_dup_fk
            THEN
               td_inst.log_msg(    'Constraint comparable to '
                                || c_constraints.constraint_name
                                || ' already exists on table '
                                || l_tab_name,
                                3
                              );
            WHEN e_dup_not_null
            THEN
               td_inst.log_msg
                          (    'Referenced not null constraint already exists on table '
                            || l_tab_name,
                            3
                          );
            WHEN OTHERS
            THEN
                  -- first log the error
               -- provide a backtrace from this exception handler to the next exception
               td_inst.log_err;
               RAISE;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No matching constraints found on ' || l_src_name );
      ELSE
         td_inst.log_msg(    l_con_cnt
                          || ' constraint'
                          || CASE
                                WHEN l_con_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' built on '
                          || l_tab_name
                        );
      END IF;

      o_ev.clear_app_info;
   END build_constraints;

   -- disables constraints related to a particular table
   PROCEDURE constraint_maint(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_maint_type          VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   )
   IS
      l_con_cnt    NUMBER         := 0;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_rows       BOOLEAN        := FALSE;
      e_iot_shc    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_iot_shc, -25188 );
      o_ev         evolve_ot         := evolve_ot( p_module => 'constraint_maint' );
   BEGIN
      -- P_CONSTRAINT_TYPE only relates to constraints based on the table, not the reference
      IF REGEXP_LIKE( 'reference|all', p_basis, 'i' ) AND p_constraint_type IS NOT NULL
      THEN
         td_inst.log_msg
            ( 'A value provided in P_CONSTRAINT_TYPE is ignored for constraints based on references'
            );
      END IF;

      -- confirm that the table exists
      -- raise an error if it doesn't
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- disable both table and reference constraints for this particular table
      o_ev.change_action( 'Constraint maintenance' );

      FOR c_constraints IN
         ( SELECT *
            FROM ( SELECT owner table_owner, table_name, constraint_name,
                             'alter table '
                          || l_tab_name
                          || ' disable constraint '
                          || constraint_name disable_ddl,
                             'Constraint '
                          || constraint_name
                          || ' disabled on '
                          || l_tab_name disable_msg,
                             'alter table '
                          || l_tab_name
                          || ' enable constraint '
                          || constraint_name enable_ddl,
                             'Constraint '
                          || constraint_name
                          || ' enabled on '
                          || l_tab_name enable_msg,
                          CASE
                             WHEN REGEXP_LIKE( 'table|all', p_basis, 'i' )
                                THEN 'Y'
                             ELSE 'N'
                          END include
                    FROM all_constraints
                   WHERE table_name = UPPER( p_table )
                     AND owner = UPPER( p_owner )
                     AND status =
                            CASE
                               WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                  THEN 'ENABLED'
                               WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                  THEN 'DISABLED'
                            END
                     AND REGEXP_LIKE( constraint_name,
                                      NVL( p_constraint_regexp, '.' ),
                                      'i'
                                    )
                     AND REGEXP_LIKE( constraint_type, NVL( p_constraint_type, '.' ), 'i' )
                  UNION
                  SELECT owner table_owner, table_name, constraint_name,
                            'alter table '
                         || owner
                         || '.'
                         || table_name
                         || ' disable constraint '
                         || constraint_name disable_ddl,
                            'Constraint '
                         || constraint_name
                         || ' disabled on '
                         || owner
                         || '.'
                         || table_name disable_msg,
                            'alter table '
                         || owner
                         || '.'
                         || table_name
                         || ' enable constraint '
                         || constraint_name enable_ddl,
                            'Constraint '
                         || constraint_name
                         || ' enabled on '
                         || owner
                         || '.'
                         || table_name enable_msg,
                         CASE
                            WHEN REGEXP_LIKE( 'reference|all', p_basis, 'i' )
                               THEN 'Y'
                            ELSE 'N'
                         END include
                    FROM all_constraints
                   WHERE constraint_type = 'R'
                     AND status =
                            CASE
                               WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                  THEN 'ENABLED'
                               WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                  THEN 'DISABLED'
                            END
                     AND REGEXP_LIKE( constraint_name,
                                      NVL( p_constraint_regexp, '.' ),
                                      'i'
                                    )
                     AND r_constraint_name IN(
                            SELECT constraint_name
                              FROM all_constraints
                             WHERE table_name = UPPER( p_table )
                               AND owner = UPPER( p_owner )
                               AND constraint_type = 'P' ))
           WHERE include = 'Y' )
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;

         BEGIN
            td_sql.exec_sql
               ( p_sql       => CASE
                    WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                       THEN c_constraints.disable_ddl
                    WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                       THEN c_constraints.enable_ddl
                 END,
                 p_auto      => 'yes'
               );

            -- insert records into a GTT
            -- this allows a call to ENABLE_CONSTRAINTS without parameters to only work on those that were previously disabled
            IF REGEXP_LIKE( 'disable', p_maint_type, 'i' )
            THEN
               o_ev.change_action( 'insert into td_con_maint_gtt' );

               INSERT INTO td_con_maint_gtt
                           ( table_owner, table_name,
                             constraint_name, disable_ddl,
                             disable_msg, enable_ddl,
                             enable_msg
                           )
                    VALUES ( c_constraints.table_owner, c_constraints.table_name,
                             c_constraints.constraint_name, c_constraints.disable_ddl,
                             c_constraints.disable_msg, c_constraints.enable_ddl,
                             c_constraints.enable_msg
                           );
            END IF;

            td_inst.log_msg
                         ( CASE
                              WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                 THEN c_constraints.disable_msg
                              WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                 THEN c_constraints.enable_msg
                           END,
                           3
                         );
            l_con_cnt := l_con_cnt + 1;
         EXCEPTION
            WHEN e_iot_shc
            THEN
               td_inst.log_msg
                      (    'Constraint '
                        || c_constraints.constraint_name
                        || ' is the primary key for either an IOT or a sorted hash cluster',
                        3
                      );
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg(    'No matching '
                          || CASE
                                WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                   THEN 'enabled'
                                WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                   THEN 'disabled'
                             END
                          || ' constraints found.'
                        );
      ELSE
         td_inst.log_msg(    l_con_cnt
                          || ' constraint'
                          || CASE
                                WHEN l_con_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' on or related to '
                          || l_tab_name
                          || ' '
                          || CASE
                                WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                   THEN 'disabled'
                                WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                   THEN 'enabled'
                             END
                        );
      END IF;

      o_ev.clear_app_info;
   END constraint_maint;

   -- enables constraints related to a particular table
   -- this procedure is used to just enable constraints disabled with the last call (in the current session) to DISABLE_CONSTRAINTS
   PROCEDURE enable_constraints
   IS
      l_con_cnt   NUMBER  := 0;
      l_rows      BOOLEAN := FALSE;
      o_ev        evolve_ot  := evolve_ot( p_module => 'enable_constraints' );
   BEGIN
      td_inst.log_msg( 'Enabling constraints disabled previously' );

      FOR c_cons IN ( SELECT *
                       FROM td_con_maint_gtt )
      LOOP
         BEGIN
            l_rows := TRUE;
            td_sql.exec_sql( p_sql => c_cons.enable_ddl, p_auto => 'yes' );
            td_inst.log_msg( c_cons.enable_msg );
            l_con_cnt := l_con_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No previously disabled constraints found' );
      ELSE
         td_inst.log_msg(    l_con_cnt
                          || ' constraint'
                          || CASE
                                WHEN l_con_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' enabled'
                        );
      END IF;

      -- commit is required to clear out the contents of the global temporary table
      COMMIT;
      o_ev.clear_app_info;
   END enable_constraints;

   -- drop particular indexes from a table
   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL
   )
   IS
      l_rows       BOOLEAN        := FALSE;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_idx_cnt    NUMBER         := 0;
      e_pk_idx     EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_pk_idx, -2429 );
      o_ev         evolve_ot         := evolve_ot( p_module => 'drop_indexes' );
   BEGIN
      FOR c_indexes IN ( SELECT 'drop index ' || owner || '.' || index_name index_ddl,
                                index_name, table_name, owner,
                                owner || '.' || index_name full_index_name
                          FROM all_indexes
                         WHERE table_name = UPPER( p_table )
                           AND table_owner = UPPER( p_owner )
                           AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
                           AND REGEXP_LIKE( index_type,
                                            '^' || NVL( p_index_type, '.' ),
                                            'i'
                                          ))
      LOOP
         l_rows := TRUE;

         BEGIN
            td_sql.exec_sql( p_sql => c_indexes.index_ddl, p_auto => 'yes' );
            l_idx_cnt := l_idx_cnt + 1;
            td_inst.log_msg( 'Index ' || c_indexes.index_name || ' dropped', 3 );
         EXCEPTION
            WHEN e_pk_idx
            THEN
               NULL;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No matching indexes found on ' || l_tab_name );
      ELSE
         td_inst.log_msg(    l_idx_cnt
                          || ' index'
                          || CASE
                                WHEN l_idx_cnt = 1
                                   THEN NULL
                                ELSE 'es'
                             END
                          || ' dropped on '
                          || l_tab_name
                        );
      END IF;

      o_ev.clear_app_info;
   END drop_indexes;

   -- drop particular constraints from a table
   PROCEDURE drop_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL
   )
   IS
      l_con_cnt    NUMBER         := 0;
      l_tab_name   VARCHAR2( 61 ) := p_owner || '.' || p_table;
      l_rows       BOOLEAN        := FALSE;
      o_ev         evolve_ot         := evolve_ot( p_module => 'drop_constraints' );
   BEGIN
      -- drop constraints
      FOR c_constraints IN ( SELECT    'alter table '
                                    || owner
                                    || '.'
                                    || table_name
                                    || ' drop constraint '
                                    || constraint_name constraint_ddl,
                                    constraint_name, table_name
                              FROM all_constraints
                             WHERE table_name = UPPER( p_table )
                               AND owner = UPPER( p_owner )
                               AND REGEXP_LIKE( constraint_name,
                                                NVL( p_constraint_regexp, '.' ),
                                                'i'
                                              )
                               AND REGEXP_LIKE( constraint_type,
                                                NVL( p_constraint_type, '.' ),
                                                'i'
                                              ))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         td_sql.exec_sql( p_sql => c_constraints.constraint_ddl, p_auto => 'yes' );
         l_con_cnt := l_con_cnt + 1;
         td_inst.log_msg( 'Constraint ' || c_constraints.constraint_name || ' dropped',
                          3 );
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No matching constraints found on ' || l_tab_name );
      ELSE
         td_inst.log_msg(    l_con_cnt
                          || ' constraint'
                          || CASE
                                WHEN l_con_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' dropped on '
                          || l_tab_name
                        );
      END IF;

      o_ev.clear_app_info;
   END drop_constraints;

   -- extracts grants for a particular object from the dictionary and applies those grants to another object
   PROCEDURE object_grants(
      p_owner           VARCHAR2,
      p_object          VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_grant_regexp    VARCHAR2 DEFAULT NULL
   )
   IS
      l_ddl         LONG;
      l_grant_cnt   NUMBER          := 0;
      l_obj_name    VARCHAR2( 61 )  := UPPER( p_owner || '.' || p_object );
      l_src_name    VARCHAR2( 61 )  := UPPER( p_source_owner || '.' || p_source_object );
      l_none_msg    VARCHAR2( 100 )
                               := 'No matching object privileges found on ' || l_src_name;
      l_rows        BOOLEAN         := FALSE;
      e_no_grants   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grants, -31608 );
      o_ev          evolve_ot          := evolve_ot( p_module => 'object_grants' );
   BEGIN
      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_sql.check_object( p_owner => p_owner, p_object => p_object );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_sql.check_object( p_owner => p_source_owner, p_object => p_source_object );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SQLTERMINATOR',
                                         FALSE
                                       );
      o_ev.change_action( 'Extract grants' );
      -- we need the sql terminator now because it will be our split character later
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,
                                         'SQLTERMINATOR',
                                         TRUE
                                       );

      -- create a cursor containing the DDL from the target indexes
      BEGIN
         -- need to remove the last sql terminator because it's a splitter between statements
         -- also remove all the extract spaces and carriage returns
         SELECT REGEXP_REPLACE( REGEXP_REPLACE( DDL, ' *\s+ +', NULL ), ';\s*$', NULL )
           INTO l_ddl
           FROM ( SELECT ( REGEXP_REPLACE
                              ( REGEXP_REPLACE
                                      ( DBMS_METADATA.get_dependent_ddl( 'OBJECT_GRANT',
                                                                         object_name,
                                                                         owner
                                                                       ),
                                           '(\."?)('
                                        || UPPER( p_source_object )
                                        || ')(\w*)("?)',
                                        '.' || UPPER( p_object ) || '\3',
                                        1,
                                        0,
                                        'i'
                                      ),
                                '(")?(' || UPPER( p_source_owner ) || ')("?\.)',
                                UPPER( p_owner ) || '.',
                                1,
                                0,
                                'i'
                              )
                         ) DDL,
                         owner object_owner, object_name
                   FROM all_objects ao
                  WHERE object_name = UPPER( p_source_object )
                    AND owner = UPPER( p_source_owner )
                    AND subobject_name IS NULL )
          -- USE an NVL'd regular expression to determine the specific indexes to work on
          -- when nothing is passed for p_INDEX_TYPE, then that is the same as passing a wildcard
         WHERE  REGEXP_LIKE( DDL, NVL( p_grant_regexp, '.' ), 'i' );
      EXCEPTION
         -- if a duplicate column list of indexes already exist, log it, but continue
         WHEN e_no_grants
         THEN
            td_inst.log_msg( l_none_msg, 3 );
      END;

      -- now, parse the string to work on the different values in it
      o_ev.change_action( 'Execute grants' );

      FOR c_grants IN ( SELECT *
                         FROM TABLE( td_ext.SPLIT( l_ddl, ';' )))
      LOOP
         l_rows := TRUE;
         td_sql.exec_sql( p_sql => c_grants.COLUMN_VALUE, p_auto => 'yes' );
         l_grant_cnt := l_grant_cnt + 1;
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( l_none_msg );
      ELSE
         td_inst.log_msg(    l_grant_cnt
                          || ' privilege'
                          || CASE
                                WHEN l_grant_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' granted on '
                          || l_obj_name
                        );
      END IF;

      o_ev.clear_app_info;
   END object_grants;

   -- structures an insert or insert append statement from the source to the target provided
   PROCEDURE insert_table(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited'
   )
   IS
      l_src_name   VARCHAR2( 61 ) := UPPER( p_source_owner || '.' || p_source_object );
      l_trg_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_results    NUMBER;
      o_ev         evolve_ot
         := evolve_ot( p_module      => 'insert_table',
                    p_action      => 'Check existence of objects' );
   BEGIN
      -- check information about the table
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- check that the source object exists.
      td_sql.check_object( p_owner            => p_source_owner,
                           p_object           => p_source_object,
                           p_object_type      => 'table$|view'
                         );

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF td_ext.is_true( p_direct ) AND p_log_table IS NOT NULL
      THEN
         td_inst.log_msg
            ( 'Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
              3
            );
      END IF;

      IF td_ext.is_true( p_trunc )
      THEN
         -- truncate the target table
         truncate_table( p_owner, p_table );
      END IF;

      -- enable|disable parallel dml depending on the parameter for P_DIRECT
      td_sql.exec_sql(    'ALTER SESSION '
                       || CASE
                             WHEN REGEXP_LIKE( 'yes', p_direct, 'i' )
                                THEN 'ENABLE'
                             ELSE 'DISABLE'
                          END
                       || ' PARALLEL DML'
                     );
      l_results :=
         td_sql.exec_sql
                   ( p_sql      =>    'insert '
                                   || CASE
                                         WHEN td_ext.is_true( p_direct )
                                            THEN '/*+ APPEND */ '
                                         ELSE NULL
                                      END
                                   || 'into '
                                   || l_trg_name
                                   || ' select '
                                   || CASE
                                         -- just use a regular expression to remove the APPEND hint if P_DIRECT is disabled
                                      WHEN p_degree IS NOT NULL
                                            THEN    '/*+ PARALLEL (source '
                                                 || p_degree
                                                 || ') */ '
                                         ELSE NULL
                                      END
                                   || '* from '
                                   || l_src_name
                                   || ' source'
                                   -- if a logging table is specified, then just append it on the end
                                   || CASE
                                         WHEN p_log_table IS NULL
                                            THEN NULL
                                         ELSE    ' log errors into '
                                              || p_log_table
                                              || ' reject limit '
                                              || p_reject_limit
                                      END
                   );

      -- record the number of rows affected
      IF NOT td_inst.is_debugmode
      THEN
         td_inst.log_cnt_msg( p_count      => l_results,
                              p_msg        =>    'Number of records inserted into '
                                              || l_trg_name
                            );
      END IF;

      o_ev.clear_app_info;
   END insert_table;

   -- structures a merge statement between two tables that have the same table
   PROCEDURE merge_table(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_columns         VARCHAR2 DEFAULT NULL,
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited'
   )
   IS
      l_onclause        VARCHAR2( 32000 );
      l_update          VARCHAR2( 32000 );
      l_insert          VARCHAR2( 32000 );
      l_values          VARCHAR2( 32000 );
      l_src_name        VARCHAR2( 61 )    := p_source_owner || '.' || p_source_object;
      l_trg_name        VARCHAR2( 61 )    := p_owner || '.' || p_table;
      l_results         NUMBER;
      e_no_on_columns   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_on_columns, -936 );
      o_ev              evolve_ot
         := evolve_ot( p_module      => 'merge_table',
                    p_action      => 'Check existence of objects' );
   BEGIN
      -- check information about the table
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- check that the source object exists.
      td_sql.check_object( p_owner            => p_source_owner,
                           p_object           => p_source_object,
                           p_object_type      => 'table$|view'
                         );

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF REGEXP_LIKE( 'yes', p_direct, 'i' ) AND p_log_table IS NOT NULL
      THEN
         td_inst.log_msg
            ( 'Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
              3
            );
      END IF;

      o_ev.change_action( 'Construct MERGE ON clause' );

      -- use the columns provided in P_COLUMNS.
      -- if that is left null, then choose the columns in the primary key of the target table
      -- if there is no primary key, then choose a unique key (any unique key)
      IF p_columns IS NOT NULL
      THEN
         WITH DATA AS
              
              -- this allows us to create a variable IN LIST based on multiple column names provided
              ( SELECT    TRIM( SUBSTR( COLUMNS,
                                        INSTR( COLUMNS, ',', 1, LEVEL ) + 1,
                                          INSTR( COLUMNS, ',', 1, LEVEL + 1 )
                                        - INSTR( COLUMNS, ',', 1, LEVEL )
                                        - 1
                                      )
                              ) AS token
                     FROM ( SELECT ',' || p_columns || ',' COLUMNS
                             FROM DUAL )
               CONNECT BY LEVEL <=
                               LENGTH( p_columns )
                             - LENGTH( REPLACE( p_columns, ',', '' ))
                             + 1 )
         SELECT REGEXP_REPLACE(    '('
                                || stragg(    'target.'
                                           || column_name
                                           || ' = source.'
                                           || column_name
                                         )
                                || ')',
                                ',',
                                ' AND' || CHR( 10 )
                              ) LIST
           INTO l_onclause
           FROM all_tab_columns
          WHERE table_name = UPPER( p_table )
            AND owner = UPPER( p_owner )
            -- select from the variable IN LIST
            AND column_name IN( SELECT *
                                 FROM DATA );
      ELSE
         -- otherwise, we need to get a constraint name
         -- we first choose a PK if it exists
         -- otherwise get a UK at random
         SELECT LIST
           INTO l_onclause
           FROM ( SELECT REGEXP_REPLACE(    '('
                                         || stragg(    'target.'
                                                    || column_name
                                                    || ' = source.'
                                                    || column_name
                                                  )
                                         || ')',
                                         ',',
                                         ' AND' || CHR( 10 )
                                       ) LIST,
                         
                         -- the MIN function will ensure that primary keys are selected first
                         -- otherwise, it will randonmly choose a remaining constraint to use
                         MIN( dc.constraint_type ) con_type
                   FROM all_cons_columns dcc JOIN all_constraints dc
                        USING( constraint_name, table_name )
                  WHERE table_name = UPPER( p_table )
                    AND dcc.owner = UPPER( p_owner )
                    AND dc.constraint_type IN( 'P', 'U' ));
      END IF;

      o_ev.change_action( 'Construct MERGE update clause' );

      IF p_columns IS NOT NULL
      THEN
         SELECT REGEXP_REPLACE( stragg(    'target.'
                                        || column_name
                                        || ' = source.'
                                        || column_name
                                      ),
                                ',',
                                ',' || CHR( 10 )
                              )
           INTO l_update
           -- if P_COLUMNS is provided, we use the same logic from the ON clause
           -- to make sure those same columns are not inlcuded in the update clause
           -- MINUS gives us that
         FROM   ( WITH DATA AS
                       ( SELECT    TRIM( SUBSTR( COLUMNS,
                                                 INSTR( COLUMNS, ',', 1, LEVEL ) + 1,
                                                   INSTR( COLUMNS, ',', 1, LEVEL + 1 )
                                                 - INSTR( COLUMNS, ',', 1, LEVEL )
                                                 - 1
                                               )
                                       ) AS token
                              FROM ( SELECT ',' || p_columns || ',' COLUMNS
                                      FROM DUAL )
                        CONNECT BY LEVEL <=
                                        LENGTH( p_columns )
                                      - LENGTH( REPLACE( p_columns, ',', '' ))
                                      + 1 )
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
                 MINUS
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table )
                    AND owner = UPPER( p_owner )
                    AND column_name IN( SELECT *
                                         FROM DATA ));
      ELSE
         -- otherwise, we once again MIN a constraint type to ensure it's the same constraint
         -- then, we just minus the column names so they aren't included
         SELECT REGEXP_REPLACE( stragg(    'target.'
                                        || column_name
                                        || ' = source.'
                                        || column_name
                                      ),
                                ',',
                                ',' || CHR( 10 )
                              )
           INTO l_update
           FROM ( SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
                 MINUS
                 SELECT column_name
                   FROM ( SELECT  column_name, MIN( dc.constraint_type ) con_type
                             FROM all_cons_columns dcc JOIN all_constraints dc
                                  USING( constraint_name, table_name )
                            WHERE table_name = UPPER( p_table )
                              AND dcc.owner = UPPER( p_owner )
                              AND dc.constraint_type IN( 'P', 'U' )
                         GROUP BY column_name ));
      END IF;

      o_ev.change_action( 'Construnct MERGE insert clause' );

      SELECT   REGEXP_REPLACE( '(' || stragg( 'target.' || column_name ) || ') ',
                               ',',
                               ',' || CHR( 10 )
                             ) LIST
          INTO l_insert
          FROM all_tab_columns
         WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
      ORDER BY column_name;

      o_ev.change_action( 'Construct MERGE values clause' );
      l_values := REGEXP_REPLACE( l_insert, 'target.', 'source.' );

      BEGIN
         o_ev.change_action( 'Issue MERGE statement' );
         -- ENABLE|DISABLE parallel dml depending on the value of P_DIRECT
         td_sql.exec_sql( p_sql      =>    'ALTER SESSION '
                                        || CASE
                                              WHEN REGEXP_LIKE( 'yes', p_direct, 'i' )
                                                 THEN 'ENABLE'
                                              ELSE 'DISABLE'
                                           END
                                        || ' PARALLEL DML'
                        );
         -- we put the merge statement together using all the different clauses constructed above
         l_results :=
            td_sql.exec_sql
                      ( p_sql      =>    'MERGE INTO '
                                      || p_owner
                                      || '.'
                                      || p_table
                                      || ' target using '
                                      || CHR( 10 )
                                      || '(select '
                                      || CASE
                                            -- just use a regular expression to remove the APPEND hint if P_DIRECT is disabled
                                         WHEN p_degree IS NOT NULL
                                               THEN    '/*+ PARALLEL (src '
                                                    || p_degree
                                                    || ') */ '
                                            ELSE NULL
                                         END
                                      || '* from '
                                      || p_source_owner
                                      || '.'
                                      || p_source_object
                                      || ' src ) source on '
                                      || CHR( 10 )
                                      || l_onclause
                                      || CHR( 10 )
                                      || ' WHEN MATCHED THEN UPDATE SET '
                                      || CHR( 10 )
                                      || l_update
                                      || CHR( 10 )
                                      || ' WHEN NOT MATCHED THEN INSERT '
                                      || CASE
                                            WHEN td_ext.is_true( p_direct )
                                               THEN '/*+ APPEND */ '
                                            ELSE NULL
                                         END
                                      || CHR( 10 )
                                      || l_insert
                                      || CHR( 10 )
                                      || ' VALUES '
                                      || CHR( 10 )
                                      || l_values
                                      -- if we specify a logging table, append that on the end
                                      || CASE
                                            WHEN p_log_table IS NULL
                                               THEN NULL
                                            ELSE    'log errors into '
                                                 || p_log_table
                                                 || ' reject limit '
                                                 -- if no reject limit is specified, then use unlimited
                                                 || p_reject_limit
                                         END
                      );
      EXCEPTION
         -- ON columns not specified correctly
         WHEN e_no_on_columns
         THEN
            raise_application_error( td_inst.get_err_cd( 'on_clause_missing' ),
                                     td_inst.get_err_msg( 'on_clause_missing' )
                                   );
      END;

      -- record the number of rows affected
      IF NOT td_inst.is_debugmode
      THEN
         td_inst.log_cnt_msg( p_count      => l_results,
                              p_msg        =>    'Number of records merged into '
                                              || l_trg_name
                            );
      END IF;

      o_ev.clear_app_info;
   END merge_table;

   -- queries the dictionary based on regular expressions and loads tables using either the load_tab method or the merge_tab method
   PROCEDURE load_tables(
      p_owner           VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_regexp   VARCHAR2,
      p_suffix          VARCHAR2 DEFAULT NULL,
      p_merge           VARCHAR2 DEFAULT 'no',
      p_part_tabs       VARCHAR2 DEFAULT 'yes',
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_commit          VARCHAR2 DEFAULT 'yes'
   )
   IS
      l_rows   BOOLEAN := FALSE;
      o_ev     evolve_ot  := evolve_ot( p_module => 'load_tables' );
   BEGIN
      -- dynamic cursor contains source and target objects
      FOR c_objects IN ( SELECT o.owner src_owner, object_name src, t.owner targ_owner,
                                table_name targ
                          FROM all_objects o JOIN all_tables t
                               ON( REGEXP_REPLACE( object_name, '([^_]+)(_)([^_]+)$',
                                                   '\1' ) =
                                      REGEXP_REPLACE( table_name,
                                                      CASE
                                                         WHEN p_suffix IS NULL
                                                            THEN '?'
                                                         ELSE '_' || p_suffix || '$'
                                                      END,
                                                      NULL
                                                    )
                                 )
                         WHERE REGEXP_LIKE( object_name, p_source_regexp, 'i' )
                           AND REGEXP_LIKE( table_name,
                                            CASE
                                               WHEN p_suffix IS NULL
                                                  THEN '?'
                                               ELSE '_' || p_suffix || '$'
                                            END,
                                            'i'
                                          )
                           AND o.owner = UPPER( p_source_owner )
                           AND t.owner = UPPER( p_owner )
                           AND o.object_type IN( 'TABLE', 'VIEW', 'SYNONYM' )
                           AND object_name <>
                                          CASE
                                             WHEN o.owner = t.owner
                                                THEN table_name
                                             ELSE NULL
                                          END
                           AND partitioned <>
                                  CASE
                                     WHEN REGEXP_LIKE( 'no', p_part_tabs, 'i' )
                                        THEN NULL
                                     WHEN REGEXP_LIKE( 'yes', p_part_tabs, 'i' )
                                        THEN 'YES'
                                  END )
      LOOP
         l_rows := TRUE;

         -- use the load_tab or merge_tab procedure depending on P_MERGE
         CASE
            WHEN td_ext.is_true( p_trunc )
            THEN
               merge_table( p_source_owner       => c_objects.src_owner,
                            p_source_object      => c_objects.src,
                            p_owner              => c_objects.targ_owner,
                            p_table              => c_objects.targ,
                            p_direct             => p_direct,
                            p_degree             => p_degree
                          );
            WHEN NOT td_ext.is_true( p_trunc )
            THEN
               insert_table( p_source_owner       => c_objects.src_owner,
                             p_source_object      => c_objects.src,
                             p_owner              => c_objects.targ_owner,
                             p_table              => c_objects.targ,
                             p_direct             => p_direct,
                             p_degree             => p_degree,
                             p_trunc              => p_trunc
                           );
         END CASE;

         -- whether or not to commit after each table
         IF REGEXP_LIKE( 'yes', p_commit, 'i' )
         THEN
            COMMIT;
         END IF;
      END LOOP;

      IF NOT l_rows
      THEN
         raise_application_error( td_inst.get_err_cd( 'incorrect_parameters' ),
                                  td_inst.get_err_msg( 'incorrect_parameters' )
                                );
      END IF;

      o_ev.clear_app_info;
   END load_tables;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_partition(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_statistics     VARCHAR2 DEFAULT 'transfer',
      p_statpercent    NUMBER DEFAULT NULL,
      p_statdegree     NUMBER DEFAULT NULL,
      p_statmethod     VARCHAR2 DEFAULT NULL
   )
   IS
      l_src_name       VARCHAR2( 61 ) := UPPER( p_source_owner || '.' || p_source_table );
      l_tab_name       VARCHAR2( 61 )               := UPPER( p_owner || '.' || p_table );
      l_target_owner   all_tab_partitions.table_name%TYPE       := p_source_owner;
      l_rows           BOOLEAN                                  := FALSE;
      l_partname       all_tab_partitions.partition_name%TYPE;
      l_ddl            LONG;
      l_build_cons     BOOLEAN                                  := FALSE;
      l_compress       BOOLEAN                                  := FALSE;
      l_dis_fkeys      BOOLEAN                                  := FALSE;
      l_retry_ddl      BOOLEAN                                  := FALSE;
      e_no_stats       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      e_compress       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_compress, -14646 );
      e_fkeys          EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_fkeys, -2266 );
      o_ev             evolve_ot               := evolve_ot( p_module      => 'exchange_partition' );
   BEGIN
      o_ev.change_action( 'Determine partition to use' );
      -- check to make sure the target table exists, is partitioned, and the partition name exists
      td_sql.check_table( p_owner            => p_owner,
                          p_table            => p_table,
                          p_partname         => p_partname,
                          p_partitioned      => 'yes'
                        );
      -- check to make sure the source table exists and is not partitioned
      td_sql.check_table( p_owner            => p_source_owner,
                          p_table            => p_source_table,
                          p_partitioned      => 'no'
                        );

      -- use either the value for P_PARTNAME or the max partition
      SELECT NVL( UPPER( p_partname ), partition_name )
        INTO l_partname
        FROM all_tab_partitions
       WHERE table_name = UPPER( p_table )
         AND table_owner = UPPER( p_owner )
         AND partition_position IN(
                     SELECT MAX( partition_position )
                       FROM all_tab_partitions
                      WHERE table_name = UPPER( p_table )
                        AND table_owner = UPPER( p_owner ));

      -- we want to gather statistics
      -- we gather statistics first before the indexes are built
      -- the indexes will collect there own statistics when they are built
      -- that is why we don't cascade
      CASE
         WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
         THEN
            update_stats( p_owner        => p_source_owner,
                          p_table        => p_source_table,
                          p_percent      => p_statpercent,
                          p_degree       => p_statdegree,
                          p_method       => p_statmethod,
                          p_cascade      => 'no'
                        );
         -- we want to transfer the statistics from the current segment into the new segment
         -- this is preferable if automatic stats are handling stats collection
         -- and you want the load time not to suffer from statistics gathering
      WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
         THEN
            update_stats( p_owner                => p_source_owner,
                          p_table                => p_source_table,
                          p_source_partname      => l_partname,
                          p_source_owner         => p_owner,
                          p_source_table         => p_table
                        );
         -- do nothing with stats
         -- this is preferable if stats are gathered on the staging segment prior to being exchanged in
         -- OWB can do this, for example
      WHEN REGEXP_LIKE( 'ignore', p_statistics, 'i' )
         THEN
            NULL;
         ELSE
            raise_application_error( td_ext.get_err_cd( 'unrecognized_parm' ),
                                        td_ext.get_err_msg( 'unrecognized_parm' )
                                     || ' : '
                                     || p_statistics
                                   );
      END CASE;

      -- now build the indexes
      -- indexes will get fresh new statistics
      -- that is why we didn't mess with these above
      build_indexes( p_owner             => p_source_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_part_type         => 'local',
                     p_tablespace        => p_index_space,
                     p_partname          => CASE
                        WHEN p_index_space IS NOT NULL
                           THEN NULL
                        ELSE l_partname
                     END
                   );
      -- now exchange the table
      o_ev.change_action( 'Exchange table' );

      -- have several exceptions that we want to handle when an exchange fails
      -- so we are using an EXIT WHEN loop
      -- if an exception that we handle is raised, then we want to rerun the exchange
      -- will try the exchange multiple times until it either succeeds, or an unrecognized exception is raised
      LOOP
         l_retry_ddl := FALSE;

         BEGIN
            td_sql.exec_sql
                ( p_sql       =>    'alter table '
                                 || l_tab_name
                                 || ' exchange partition '
                                 || l_partname
                                 || ' with table '
                                 || l_src_name
                                 || ' including indexes without validation update global indexes',
                  p_auto      => 'yes'
                );
            td_inst.log_msg(    l_src_name
                             || ' exchanged for partition '
                             || l_partname
                             || ' of table '
                             || l_tab_name
                           );
         EXCEPTION
            WHEN e_fkeys
            THEN
               -- disable foreign keys related to the table
               -- this will enable the exchange to occur
               o_ev.change_action( 'Disable foreign keys' );
               l_dis_fkeys := TRUE;
               l_retry_ddl := TRUE;
               constraint_maint( p_owner           => p_owner,
                                 p_table           => p_table,
                                 p_maint_type      => 'disable',
                                 p_basis           => 'reference'
                               );
            WHEN e_compress
            THEN
               td_inst.log_msg( l_src_name || ' compressed to facilitate exchange', 3 );
               -- need to compress the staging table
               l_compress := TRUE;
               l_retry_ddl := TRUE;
               td_sql.exec_sql( p_sql       =>    'alter table '
                                               || l_src_name
                                               || ' move compress',
                                p_auto      => 'yes'
                              );
            WHEN OTHERS
            THEN
                  -- first log the error
               -- provide a backtrace from this exception handler to the next exception
               td_inst.log_err;

               -- need to drop indexes if there is an exception
               -- this is for rerunability
               IF td_ext.is_true( p_index_drop )
               THEN
                  -- now record the reason for the index drops
                  td_inst.log_msg( 'Dropping indexes for restartability', 3 );
                  drop_indexes( p_owner => p_source_owner, p_table => p_source_table );
               END IF;

               -- need to put the disabled foreign keys back if we disabled them
               IF l_dis_fkeys
               THEN
                  enable_constraints;
               END IF;

               RAISE;
         END;

         EXIT WHEN NOT l_retry_ddl;
      END LOOP;

      -- enable any foreign keys on other tables that reference this table
      IF l_dis_fkeys
      THEN
         o_ev.change_action( 'Enable foreign keys' );
         constraint_maint( p_owner           => p_owner,
                           p_table           => p_table,
                           p_maint_type      => 'enable',
                           p_basis           => 'reference'
                         );
      END IF;

      -- drop the indexes on the stage table
      IF td_ext.is_true( p_index_drop )
      THEN
         drop_indexes( p_owner => p_source_owner, p_table => p_source_table );
      END IF;

      o_ev.clear_app_info;
   END exchange_partition;

   -- procedure to "swap" two tables using rename
   PROCEDURE replace_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   )
   IS
      l_src_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_source_table );
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_tab_rn     VARCHAR2( 30 ) := UPPER( 'td$' || SUBSTR( p_table, 1, 25 ) || '_rn' );
      l_ren_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || l_tab_rn );
      l_rows       BOOLEAN        := FALSE;
      l_ddl        LONG;
      e_no_stats   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      e_compress   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_compress, -14646 );
      e_fkeys      EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_fkeys, -2266 );
      o_ev         evolve_ot         := evolve_ot( p_module => 'replace_table' );
   BEGIN
      o_ev.change_action( 'Perform object checks' );
      -- check to make sure the target table exists
      td_sql.check_table( p_owner => p_owner, p_table => p_table );
      -- check to make sure the source table exists
      td_sql.check_table( p_owner => p_owner, p_table => p_source_table );

      -- do something with statistics on the new table
      -- if p_statistics is 'ignore', then do nothing
      IF REGEXP_LIKE( 'ignore', p_statistics, 'i' )
      THEN
         NULL;
      ELSE
         -- otherwise, we will either gather or transfer statistics
         -- this depends on the value of p_statistics
         -- will be building indexes later, which gather their own statistics
         -- so P_CASCADE is fales
         update_stats( p_owner             => p_owner,
                       p_table             => p_table,
                       p_source_owner      => CASE
                          WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
                             THEN NULL
                          WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
                             THEN p_owner
                       END,
                       p_source_table      => CASE
                          WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
                             THEN NULL
                          WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
                             THEN p_table
                       END,
                       p_cascade           => 'no'
                     );
      END IF;

      -- build the indexes
      build_indexes( p_owner             => p_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_tablespace        => p_tablespace
                   );
      -- build the constraints
      build_constraints( p_owner             => p_owner,
                         p_table             => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_table      => p_table,
                         p_tablespace        => p_tablespace
                       );
      -- grant privileges
      object_grants( p_owner              => p_owner,
                     p_object             => p_source_table,
                     p_source_owner       => p_owner,
                     p_source_object      => p_table
                   );
      -- now replace the table
      -- using a table rename for this
      o_ev.change_action( 'Rename tables' );
      -- first name the current table to another name
      td_sql.exec_sql( p_sql       =>    'alter table '
                                      || l_tab_name
                                      || ' rename to '
                                      || l_tab_rn,
                       p_auto      => 'yes'
                     );
      -- now rename to source table to the target table
      td_sql.exec_sql( p_sql       =>    'alter table '
                                      || l_src_name
                                      || ' rename to '
                                      || UPPER( p_table ),
                       p_auto      => 'yes'
                     );
      -- now rename to previous target table to the source table name
      td_sql.exec_sql( p_sql       =>    'alter table '
                                      || l_ren_name
                                      || ' rename to '
                                      || UPPER( p_source_table ),
                       p_auto      => 'yes'
                     );

      -- drop the indexes on the stage table
      IF td_ext.is_true( p_index_drop )
      THEN
         drop_indexes( p_owner => p_owner, p_table => p_source_table );
      END IF;

      -- rename the indexes
      rename_indexes;
      -- clear out temporary table holding index statements
      COMMIT;
      o_ev.clear_app_info;
   END replace_table;

   -- Provides functionality for setting local and non-local indexes to unusable based on parameters
   -- Can also base which index partitions to mark as unuable based on the contents of another table
   -- There are two "magic" numbers that are required to make it work correctly.
   -- The defaults will quite often work.
   -- The simpliest way to find which magic numbers make this function work is to
   -- do a partition exchange on the target table and trace that statement.
   PROCEDURE unusable_indexes(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 0,
      p_p_num           NUMBER DEFAULT 65535,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL
   )
   IS
      l_tab_name   VARCHAR2( 61 )   := UPPER( p_owner ) || '.' || UPPER( p_table );
      l_src_name   VARCHAR2( 61 )
                             := UPPER( p_source_owner ) || '.'
                                || UPPER( p_source_object );
      l_msg        VARCHAR2( 2000 );
      l_ddl        VARCHAR2( 2000 );
      l_pidx_cnt   NUMBER;
      l_idx_cnt    NUMBER;
      l_rows       BOOLEAN          DEFAULT FALSE;
      o_ev         evolve_ot           := evolve_ot( p_module => 'unusable_indexes' );
   BEGIN
      CASE
         WHEN     p_partname IS NOT NULL
              AND ( p_source_owner IS NOT NULL OR p_source_object IS NOT NULL )
         THEN
            raise_application_error
                            ( td_inst.get_err_cd( 'parms_not_compatible' ),
                                 td_inst.get_err_msg( 'parms_not_compatible' )
                              || ': P_PARTNAME with either P_SOURCE_OWNER or P_SOURCE_OBJECT'
                            );
         WHEN p_source_owner IS NOT NULL AND p_source_object IS NULL
         THEN
            raise_application_error( td_inst.get_err_cd( 'parms_not_compatible' ),
                                        td_inst.get_err_msg( 'parms_not_compatible' )
                                     || ': P_SOURCE_OWNER without P_SOURCE_OBJECT'
                                   );
         WHEN p_source_owner IS NULL AND p_source_object IS NOT NULL
         THEN
            raise_application_error( td_inst.get_err_cd( 'parms_not_compatible' ),
                                        td_inst.get_err_msg( 'parms_not_compatible' )
                                     || ': P_SOURCE_OBJECT without P_SOURCE_OWNER'
                                   );
         ELSE
            NULL;
      END CASE;

      -- test the target table
      td_sql.check_table( p_owner         => p_owner, p_table => p_table,
                          p_partname      => p_partname );

      -- test the source object
      -- but only if it's specified
      -- make sure it's a table or view
      IF p_source_object IS NOT NULL
      THEN
         td_sql.check_object( p_owner            => p_source_owner,
                              p_object           => p_source_object,
                              p_object_type      => 'table$|view'
                            );
      END IF;

      o_ev.change_action( 'Populate PARTNAME table' );

      IF p_partname IS NOT NULL OR p_source_object IS NOT NULL
      THEN
         -- populate a global temporary table with the indexes to work on
         -- this is a requirement because the dynamic SQL needed to use the tbl$or$idx$part$num function
         populate_partname( p_owner              => p_owner,
                            p_table              => p_table,
                            p_partname           => p_partname,
                            p_source_owner       => p_source_owner,
                            p_source_object      => p_source_object,
                            p_source_column      => p_source_column,
                            p_d_num              => p_d_num,
                            p_p_num              => p_p_num
                          );
      END IF;

      -- this cursor will contain all the ALTER INDEX statements necessary to mark indexes unusable
      -- the contents of the cursor depends very much on the parameters specified
      -- also depends on the contents of the PARTNAME global temporary table
      o_ev.change_action( 'Calculate indexes to affect' );

      FOR c_idx IN
         ( SELECT *
            FROM ( SELECT DISTINCT    'alter index '
                                   || owner
                                   || '.'
                                   || index_name
                                   || CASE idx_ddl_type
                                         WHEN 'I'
                                            THEN NULL
                                         ELSE ' modify partition ' || partition_name
                                      END
                                   || ' unusable' DDL,
                                   idx_ddl_type, partition_name, partition_position,
                                   SUM( CASE idx_ddl_type
                                           WHEN 'I'
                                              THEN 1
                                           ELSE 0
                                        END ) OVER( PARTITION BY 1 ) num_indexes,
                                   SUM( CASE idx_ddl_type
                                           WHEN 'P'
                                              THEN 1
                                           ELSE 0
                                        END
                                      ) OVER( PARTITION BY 1 ) num_partitions,
                                   CASE idx_ddl_type
                                      WHEN 'I'
                                         THEN ai_status
                                      ELSE aip_status
                                   END status,
                                   include
                             FROM ( SELECT index_type, owner, ai.index_name,
                                           partition_name, aip.partition_position,
                                           partitioned, aip.status aip_status,
                                           ai.status ai_status,
                                           CASE
                                              WHEN partition_name IS NULL
                                               OR partitioned = 'NO'
                                                 THEN 'I'
                                              ELSE 'P'
                                           END idx_ddl_type,
                                           CASE
                                              WHEN(    p_source_object IS NOT NULL
                                                    OR p_partname IS NOT NULL
                                                  )
                                              AND ( partitioned = 'YES' )
                                              AND partition_name IS NULL
                                                 THEN 'N'
                                              ELSE 'Y'
                                           END include
                                     FROM td_part_gtt JOIN all_ind_partitions aip
                                          USING( partition_name )
                                          RIGHT JOIN all_indexes ai
                                          ON ai.index_name = aip.index_name
                                        AND ai.owner = aip.index_owner
                                    WHERE ai.table_name = UPPER( p_table )
                                      AND ai.table_owner = UPPER( p_owner ))
                            WHERE REGEXP_LIKE( index_type, '^' || p_index_type, 'i' )
                              AND REGEXP_LIKE( partitioned,
                                               CASE
                                                  WHEN REGEXP_LIKE( 'global',
                                                                    p_part_type,
                                                                    'i'
                                                                  )
                                                     THEN 'NO'
                                                  WHEN REGEXP_LIKE( 'local',
                                                                    p_part_type,
                                                                    'i'
                                                                  )
                                                     THEN 'YES'
                                                  ELSE '.'
                                               END,
                                               'i'
                                             )
                              -- USE an NVL'd regular expression to determine specific indexes to work on
                              AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ),
                                               'i' )
                              AND NOT REGEXP_LIKE( index_type, 'iot', 'i' )
                              AND include = 'Y'
                         ORDER BY idx_ddl_type, partition_position )
           WHERE status IN( 'VALID', 'USABLE', 'N/A' ))
      LOOP
         o_ev.change_action( 'Execute index DDL' );
         l_rows := TRUE;
         td_sql.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes' );
         l_pidx_cnt := c_idx.num_partitions;
         l_idx_cnt := c_idx.num_indexes;
      END LOOP;

      IF l_rows
      THEN
         IF l_idx_cnt > 0
         THEN
            td_inst.log_msg(    l_idx_cnt
                             || ' index'
                             || CASE l_idx_cnt
                                   WHEN 1
                                      THEN NULL
                                   ELSE 'es'
                                END
                             || ' affected'
                           );
         END IF;

         IF l_pidx_cnt > 0
         THEN
            td_inst.log_msg(    l_pidx_cnt
                             || ' local index partition'
                             || CASE l_idx_cnt
                                   WHEN 1
                                      THEN NULL
                                   ELSE 's'
                                END
                             || ' affected'
                           );
         END IF;
      ELSE
         td_inst.log_msg( 'No matching usable indexes found' );
      END IF;

      -- commit needed to clear the contents of the global temporary table
      COMMIT;
      o_ev.clear_app_info;
   END unusable_indexes;

   -- rebuilds all unusable index segments on a particular table
   PROCEDURE usable_indexes(
      p_owner   VARCHAR2,                     -- owner of table for the indexes to work on
      p_table   VARCHAR2                                -- table to operate on indexes for
   )
   IS
      l_ddl    VARCHAR2( 2000 );
      l_rows   BOOLEAN          := FALSE;                       -- to catch empty cursors
      l_cnt    NUMBER           := 0;
      o_ev     evolve_ot
                 := evolve_ot( p_module      => 'usable_indexes',
                            p_action      => 'Rebuild indexes' );
   BEGIN
      td_sql.check_table( p_owner => p_owner, p_table => p_table );

      IF td_sql.is_part_table( p_owner, p_table )
      THEN
         -- rebuild local indexes first
         FOR c_idx IN ( SELECT  table_name, partition_position,
                                   'alter table '
                                || table_owner
                                || '.'
                                || table_name
                                || ' modify partition '
                                || partition_name
                                || ' rebuild unusable local indexes' DDL,
                                partition_name
                           FROM all_tab_partitions
                          WHERE table_name = UPPER( p_table )
                            AND table_owner = UPPER( p_owner )
                       ORDER BY table_name, partition_position )
         LOOP
            td_sql.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes' );
            l_cnt := l_cnt + 1;
         END LOOP;

         td_inst.log_msg(    'Any unusable indexes on '
                          || l_cnt
                          || ' table partition'
                          || CASE
                                WHEN l_cnt = 1
                                   THEN NULL
                                ELSE 's'
                             END
                          || ' rebuilt'
                        );
      END IF;

      -- reset variables
      l_cnt := 0;
      l_rows := FALSE;

      -- now see if any global are still unusable
      FOR c_gidx IN ( SELECT  table_name,
                                 'alter index '
                              || owner
                              || '.'
                              || index_name
                              || ' rebuild parallel nologging' DDL
                         FROM all_indexes
                        WHERE table_name = UPPER( p_table )
                          AND table_owner = UPPER( p_owner )
                          AND status = 'UNUSABLE'
                          AND partitioned = 'NO'
                     ORDER BY table_name )
      LOOP
         l_rows := TRUE;
         td_sql.exec_sql( p_sql => c_gidx.DDL, p_auto => 'yes' );
         l_cnt := l_cnt + 1;
      END LOOP;

      IF l_rows
      THEN
         td_inst.log_msg(    l_cnt
                          || CASE
                                WHEN td_sql.is_part_table( p_owner, p_table )
                                   THEN ' global'
                                ELSE NULL
                             END
                          || ' index'
                          || CASE l_cnt
                                WHEN 1
                                   THEN NULL
                                ELSE 'es'
                             END
                          || ' rebuilt'
                        );
      ELSE
         td_inst.log_msg(    'No matching unusable '
                          || CASE
                                WHEN td_sql.is_part_table( p_owner, p_table )
                                   THEN 'global '
                                ELSE NULL
                             END
                          || 'indexes found'
                        );
      END IF;

      o_ev.clear_app_info;
   END usable_indexes;

   PROCEDURE update_stats(
      p_owner             VARCHAR2,
      p_table             VARCHAR2 DEFAULT NULL,
      p_partname          VARCHAR2 DEFAULT NULL,
      p_source_owner      VARCHAR2 DEFAULT NULL,
      p_source_table      VARCHAR2 DEFAULT NULL,
      p_source_partname   VARCHAR2 DEFAULT NULL,
      p_percent           NUMBER DEFAULT NULL,
      p_degree            NUMBER DEFAULT NULL,
      p_method            VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_granularity       VARCHAR2 DEFAULT 'AUTO',
      p_cascade           VARCHAR2 DEFAULT NULL,
      p_options           VARCHAR2 DEFAULT 'GATHER AUTO'
   )
   IS
      l_numrows     NUMBER;
      l_numblks     NUMBER;
      l_avgrlen     NUMBER;
      l_cachedblk   NUMBER;
      l_cachehit    NUMBER;
      l_statid      VARCHAR2( 30 )
         :=    'TD$'
            || SYS_CONTEXT( 'USERENV', 'SESSIONID' )
            || TO_CHAR( SYSDATE, 'yyyymmdd_hhmiss' );
      e_no_stats    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      l_rows        BOOLEAN        := FALSE;                     -- to catch empty cursors
      o_ev          evolve_ot         := evolve_ot( p_module => 'update_stats' );
   BEGIN
      -- check all the parameter requirements
      CASE
         WHEN    p_source_owner IS NOT NULL AND p_source_table IS NULL
              OR ( p_source_owner IS NULL AND p_source_table IS NOT NULL )
         THEN
            raise_application_error
                           ( td_inst.get_err_cd( 'parms_not_compatible' ),
                                td_inst.get_err_msg( 'parms_not_compatible' )
                             || ': P_SOURCE_OWNER and P_SOURCE_OBJECT are mutually inclusive'
                           );
         WHEN     p_source_partname IS NOT NULL
              AND ( p_source_owner IS NULL OR p_source_table IS NULL )
         THEN
            raise_application_error
                       ( td_inst.get_err_cd( 'parms_not_compatible' ),
                            td_inst.get_err_msg( 'parms_not_compatible' )
                         || ': P_SOURCE_PARTNAME requires P_SOURCE_OWNER and P_SOURCE_OBJECT'
                       );
         WHEN p_partname IS NOT NULL AND( p_owner IS NULL OR p_table IS NULL )
         THEN
            raise_application_error( td_inst.get_err_cd( 'parms_not_compatible' ),
                                        td_inst.get_err_msg( 'parms_not_compatible' )
                                     || ': P_PARTNAME requires P_OWNER and P_OBJECT'
                                   );
         ELSE
            NULL;
      END CASE;

      -- verify the structure of the target table
      -- this is only applicable if a table is having stats gathered, instead of a schema
      IF p_table IS NOT NULL
      THEN
         td_sql.check_table( p_owner         => p_owner,
                             p_table         => p_table,
                             p_partname      => p_partname
                           );
      END IF;

      -- verify the structure of the source table (if specified)
      IF ( p_source_owner IS NOT NULL OR p_source_table IS NOT NULL )
      THEN
         td_sql.check_table( p_owner         => p_source_owner,
                             p_table         => p_source_table,
                             p_partname      => p_source_partname
                           );
      END IF;

      o_ev.change_action( 'Gathering statistics' );

      -- check to see if we are in debug mode
      IF NOT td_inst.is_debugmode
      THEN
         -- check to see if source owner is null
         -- if source owner is null, then we know we aren't transferring statistics
         -- so we need to gather them
         IF p_source_owner IS NULL
         THEN
            -- check to see if the table name is null
            -- if it is, then we are not gathering stats on a particular table, but instead a whole schema
            -- in that case, we need to call GATHER_SCHEMA_STATS instead of GATHER_TABLE_STATS
            IF p_table IS NULL
            THEN
               DBMS_STATS.gather_schema_stats
                                 ( ownname               => p_owner,
                                   estimate_percent      => NVL
                                                               ( p_percent,
                                                                 DBMS_STATS.auto_sample_size
                                                               ),
                                   method_opt            => p_method,
                                   DEGREE                => NVL( p_degree,
                                                                 DBMS_STATS.auto_degree
                                                               ),
                                   granularity           => p_granularity,
                                   CASCADE               => NVL
                                                               ( td_ext.is_true
                                                                              ( p_cascade,
                                                                                TRUE
                                                                              ),
                                                                 DBMS_STATS.auto_cascade
                                                               ),
                                   options               => p_options
                                 );
            -- if the table name is not null, then we are only collecting stats on a particular table
            -- will call GATHER_TABLE_STATS as opposed to GATHER_SCHEMA_STATS
            ELSE
               DBMS_STATS.gather_table_stats
                                  ( ownname               => p_owner,
                                    tabname               => p_table,
                                    estimate_percent      => NVL
                                                                ( p_percent,
                                                                  DBMS_STATS.auto_sample_size
                                                                ),
                                    method_opt            => p_method,
                                    DEGREE                => NVL( p_degree,
                                                                  DBMS_STATS.auto_degree
                                                                ),
                                    granularity           => p_granularity,
                                    CASCADE               => NVL
                                                                ( td_ext.is_true
                                                                              ( p_cascade,
                                                                                TRUE
                                                                              ),
                                                                  DBMS_STATS.auto_cascade
                                                                )
                                  );
            END IF;
         -- if the source owner isn't null, then we know we are transferring statistics
         -- we will use GET_TABLE_STATS and PUT_TABLE_STATS
         ELSE
            o_ev.change_action( 'Transfer stats' );

            -- this will either take partition level statistics and import into a table
            -- or, it will take table level statistics and import it into a partition
            -- or, it will take table level statistics and import it into a table.
            -- all of this depends on whether P_PARTNAME and P_SOURCE_PARTNAME are defined or not
            BEGIN
               DBMS_STATS.export_table_stats( ownname       => p_source_owner,
                                              tabname       => p_source_table,
                                              partname      => p_source_partname,
                                              statown       => USER,
                                              stattab       => 'OPT_STATS',
                                              statid        => l_statid
                                            );

               -- now, update the table name in the stats table to the new table name
               UPDATE opt_stats
                  SET c1 = UPPER( p_table )
                WHERE statid = l_statid;

               CASE
                  -- if the source table is partitioned
               WHEN     td_sql.is_part_table( p_owner      => p_source_owner,
                                              p_table      => p_source_table
                                            )
                    -- and the target table is not partitioned
                    AND NOT td_sql.is_part_table( p_owner      => p_owner,
                                                  p_table      => p_table )
                  -- then delete the partition level information from the stats table
               THEN
                     DELETE FROM opt_stats
                           WHERE statid = l_statid
                             AND ( c2 IS NOT NULL OR c3 IS NOT NULL );
                  ELSE
                     NULL;
               END CASE;

               -- now import the statistics
               DBMS_STATS.import_table_stats( ownname       => p_owner,
                                              tabname       => p_table,
                                              partname      => p_partname,
                                              statown       => USER,
                                              stattab       => 'OPT_STATS',
                                              statid        => l_statid
                                            );

               -- now, delete these records from the stats table
               DELETE FROM opt_stats
                     WHERE statid = l_statid;
            END;
         END IF;
      END IF;

      td_inst.log_msg(    'Statistics '
                       || CASE
                             WHEN p_source_table IS NULL
                                THEN 'gathered on '
                             ELSE    'from '
                                  || CASE
                                        WHEN p_source_partname IS NULL
                                           THEN NULL
                                        ELSE    'partition '
                                             || UPPER( p_source_partname )
                                             || ' of '
                                     END
                                  || 'table '
                                  || UPPER( p_source_owner || '.' || p_source_table )
                                  || ' transfered to '
                          END
                       || CASE
                             WHEN p_partname IS NULL
                                THEN NULL
                             ELSE 'partition ' || UPPER( p_partname ) || ' of table '
                          END
                       || CASE
                             WHEN p_table IS NULL
                                THEN 'schema '
                             ELSE NULL
                          END
                       || UPPER( p_owner )
                       || CASE
                             WHEN p_table IS NULL
                                THEN NULL
                             ELSE '.'
                          END
                       || UPPER( p_table )
                     );
      COMMIT;
      o_ev.clear_app_info;
   END update_stats;
END td_ddl;
/