CREATE OR REPLACE PACKAGE BODY td_dbutils
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
      o_ev              evolve_ot                                    := evolve_ot( p_module => 'populate_partname' );
      l_dsql            LONG;
      l_num_msg         VARCHAR2( 100 )                              := 'Number of records inserted into TD_PART_GTT table';
      l_source_column   all_part_key_columns.column_name%TYPE;
      l_results         NUMBER;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_high_value      all_tab_partitions.high_value%TYPE;
      l_num_rows        NUMBER;
   BEGIN
      td_utils.check_table( p_owner => p_owner, p_table => p_table, p_partname => p_partname, p_partitioned => 'yes' );

      -- get the default partname, which is the max partition
      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position, high_value
           INTO l_part_position, l_high_value
           FROM all_tab_partitions
          WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table )
            AND partition_name = UPPER( p_partname );

         -- write records to the global temporary table, which will later be used in cursors for other procedures

         -- if P_PARTNAME is null, then we want the max partition
         -- go ahead and write that single record
         o_ev.change_action( 'static insert' );

         INSERT INTO td_part_gtt
                     ( table_owner, table_name, partition_name, partition_position
                     )
              VALUES ( UPPER( p_owner ), UPPER( p_table ), UPPER( p_partname ), l_part_position
                     );

         evolve.log_cnt_msg( SQL%ROWCOUNT, l_num_msg, 4 );
      ELSE
         -- if P_SOURCE_COLUMN is null, then use the same name as the partitioning source column on the target
         IF p_source_column IS NULL
         THEN
            SELECT column_name
              INTO l_source_column
              FROM all_part_key_columns
             WHERE NAME = UPPER( p_table ) AND owner = UPPER( p_owner );
         ELSE
            l_source_column    := p_source_column;
         END IF;

         o_ev.change_action( 'dynamic insert' );

         EXECUTE IMMEDIATE    'insert into td_part_gtt (table_owner, table_name, partition_name, partition_position) '
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
                           || 'ORDER By partition_position';

         evolve.log_cnt_msg( SQL%ROWCOUNT, l_num_msg, 4 );
      END IF;

      -- get count of records affected
      SELECT COUNT( * )
        INTO l_num_rows
        FROM td_part_gtt;

      evolve.log_msg( 'Number of records currently in TD_PART_GTT:' || l_num_rows, 5 );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END populate_partname;
   
   PROCEDURE enqueue_ddl(
      p_stmt          VARCHAR2,
      p_msg           VARCHAR2,
      p_module        VARCHAR2,
      p_action	      VARCHAR2,
      p_order         VARCHAR2 DEFAULT NULL
   )
   AS
      o_ev              evolve_ot     := evolve_ot( p_module => 'enqueue_ddl' );
   BEGIN
         INSERT INTO ddl_queue
                     ( stmt_ddl, stmt_msg, module, action, stmt_order
                     )
              VALUES ( p_stmt, p_msg, p_module, p_action, p_order
                     );

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END enqueue_ddl;
   
   PROCEDURE dequeue_ddl( 
      p_module        VARCHAR2,
      p_action	      VARCHAR2,
      p_concurrent    VARCHAR2 DEFAULT 'no',
      p_raise_err     VARCHAR2 DEFAULT 'yes'
   )
   IS
      l_stmt_cnt         NUMBER    := 0;
      l_stmtcurrent_id   VARCHAR2( 100 ) := NULL;
      l_rows            BOOLEAN   := FALSE;
      -- purposefully not initiating an EVOLVE_OT object
      -- this procedure needs to be transparent for a reason
      o_ev              evolve_ot := evolve_ot( p_module => 'dequeue_ddl' );
   BEGIN

      -- need to get a unique "job header" number in case we are running concurrently
      IF td_core.is_true( p_concurrent )
      THEN
	 o_ev.change_action( 'get concurrent id' );
         l_stmtcurrent_id    := evolve.get_concurrent_id;
      END IF;

      o_ev.change_action( 'looping through DDL' );
      -- looping through records in the DDL_QUEUE table
      -- finding statements queueud there previously
      evolve.log_msg( 'Executing DDL statements previously queued' );
      FOR c_stmts IN ( SELECT stmt_ddl,
			     stmt_msg,
			     ROWID
			FROM ddl_queue
		       WHERE lower( module ) = lower( p_module )
			 AND lower( action ) = lower( p_action )
		       ORDER BY stmt_order )

      LOOP
         BEGIN
            l_rows       := TRUE;
            -- execute the DDL either in this session or a background session
            o_ev.change_action( 'executing DDL' );
            evolve.exec_sql( p_sql => c_stmts.stmt_ddl, p_auto => 'yes', p_concurrent_id => l_stmtcurrent_id );
            evolve.log_msg( c_stmts.stmt_msg );
	    
	    -- delete the row from the queue once it's executed
	    DELETE FROM ddl_queue
	     WHERE ROWID = c_stmts.rowid;

            l_stmt_cnt    := l_stmt_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No queued DDL statements applicable for this module and action' );
      ELSE
         -- wait for the concurrent processes to complete or fail
         o_ev.change_action( 'wait on concurrent processes' );

         IF td_core.is_true( p_concurrent )
         THEN
            evolve.coordinate_sql( p_concurrent_id => l_stmtcurrent_id, p_raise_err => 'no' );
         END IF;

         evolve.log_msg(    l_stmt_cnt
                             || ' queued DDL statement'
                             || CASE
                                   WHEN l_stmt_cnt = 1
                                      THEN NULL
                                   ELSE 's'
                                END
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'executed'
                                END
                           );
      END IF;
      
      -- commit to clear the queue
      COMMIT;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END dequeue_ddl;

   PROCEDURE truncate_table( p_owner VARCHAR2, p_table VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' )
   IS
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot      := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      -- confirm that the table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      evolve.exec_sql( p_sql       =>    'truncate table '
                                          || p_owner
                                          || '.'
                                          || p_table
                                          || CASE
                                                WHEN td_core.is_true( p_reuse )
                                                   THEN ' reuse storage'
                                                ELSE NULL
                                             END,
                           p_auto      => 'yes'
                         );
      evolve.log_msg( l_tab_name || ' truncated' );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END truncate_table;

   -- drop a table
   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2 DEFAULT 'yes' )
   IS
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot      := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      -- confirm that the table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      evolve.exec_sql( p_sql       =>    'drop table '
                                          || p_owner
                                          || '.'
                                          || p_table
                                          || CASE
                                                WHEN td_core.is_true( p_purge )
                                                   THEN ' purge'
                                                ELSE NULL
                                             END,
                           p_auto      => 'yes'
                         );
      evolve.log_msg( l_tab_name || ' dropped' );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      l_table_ddl      LONG;
      l_rename_ddl     VARCHAR2( 4000 );
      l_rename_msg     VARCHAR2( 4000 );
      l_iot_type       all_tables.iot_type%TYPE;
      l_idx_cnt        NUMBER                        := 0;
      l_tab_name       VARCHAR2( 61 )                := UPPER( p_owner || '.' || p_table );
      l_src_name       VARCHAR2( 61 )                := UPPER( p_source_owner || '.' || p_source_table );
      l_part_type      VARCHAR2( 6 );
      l_targ_part      all_tables.partitioned%TYPE;
      l_rows           BOOLEAN                       := FALSE;
      e_dup_idx_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_idx_name, -955 );
      e_dup_col_list   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_col_list, -1408 );
      o_ev             evolve_ot                     := evolve_ot( p_module => 'build_table' );
   BEGIN
      -- confirm that the source table
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table );
      -- don't want any constraints pulled
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'CONSTRAINTS', FALSE );
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', FALSE );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE );
      -- we need the segment attributes so things go where we want them to
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', TRUE );
      -- don't want all the other storage aspects though
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE', FALSE );
      o_ev.change_action( 'extract DDL' );

-- SELECT DDL into a variable
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
                                                                '\1'
                                                             || CASE generic_con
                                                                   WHEN 'Y'
                                                                      THEN con_rename_adj
                                                                   ELSE con_rename
                                                                END,
                                                             1,
                                                             0,
                                                             'i'
                                                           ),
                                             '(\.)("?)(' || p_source_table || ')("?)',
                                             '\1' || UPPER( p_table ),
                                             1,
                                             0,
                                             'i'
                                           ),
                             '(table)(\s+)("?)(' || p_source_owner || ')("?)(\.)',
                             '\1\2' || UPPER( p_owner ) || '\6',
                             1,
                             0,
                             'i'
                           ) table_ddl,
                
                    -- this column was added for the REPLACE_TABLE procedure
                -- IN that procedure, after cloning the indexes, the table is renamed
                -- we have to rename the indexes back to their original names
                ' alter table '
             || UPPER( p_source_owner || '.' || p_source_table )
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
             || UPPER( p_source_owner || '.' || p_source_table )
             || ' renamed to '
             || source_constraint rename_msg,
             iot_type
        INTO l_table_ddl,
             l_rename_ddl,
             l_rename_msg,
             l_iot_type
        FROM ( SELECT
                      -- this regular expression evaluates p_TABLESPACE and modifies the DDL accordingly
                      REGEXP_REPLACE
                          (
                            -- this regular expression evaluates p_PARTITIONING paramater and removes partitioning information if necessary
                            REGEXP_REPLACE( table_ddl,
                                            CASE td_core.get_yn_ind( p_partitioning )
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
                               WHEN p_tablespace IS NULL
                                  THEN '\1\2\3\4'
                               WHEN p_tablespace = 'default'
                                  THEN NULL
                               ELSE '\1\2' || UPPER( p_tablespace ) || '\4'
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
                              UPPER(    SUBSTR( p_table, 1, 24 )
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
                        FROM ( SELECT DBMS_METADATA.get_ddl( 'TABLE', AT.table_name, AT.owner ) table_ddl, iot_type,
                                      AT.owner source_owner, constraint_name source_constraint, constraint_type,
                                      index_owner, index_name,
                                      CASE
                                         WHEN constraint_name IS NULL
                                            THEN 'N'
                                         ELSE 'Y'
                                      END key_exists,
                                      
                                      -- this is the constraint name that will be used if it doesn't already exist
                                                -- basically, all cases of the previous table name are replaced with the new table name
                                      UPPER( REGEXP_REPLACE( constraint_name,
                                                             '(")?' || p_source_table || '(")?',
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
                                      END con_ext
                                FROM all_tables AT
                                                  -- joining here to get the primary key for the table (if it exists)
                                                  -- this is used to handle IOT's correctly
                                     LEFT JOIN all_constraints ac
                                     ON AT.table_name = ac.table_name AND AT.owner = ac.owner
                                        AND ac.constraint_type = 'P'
                               WHERE AT.owner = UPPER( p_source_owner ) AND AT.table_name = UPPER( p_source_table )) g1
                             LEFT JOIN
                             
                             -- joining here to see if the proposed constraint_name (con_rename) actually exists
                             ( SELECT owner constraint_owner_confirm, constraint_name constraint_name_confirm
                                FROM all_constraints ) g2
                             ON g1.con_rename = g2.constraint_name_confirm
                           AND g2.constraint_owner_confirm = UPPER( p_owner )
                             ));

      o_ev.change_action( 'execute DDL' );
      evolve.exec_sql( p_sql => l_table_ddl, p_auto => 'yes' );
      evolve.log_msg( 'Table ' || l_tab_name || ' created' );

      -- if you want the records as well
      IF td_core.is_true( p_rows )
      THEN
         o_ev.change_action( 'insert rows' );
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
            o_ev.clear_app_info;
            evolve.raise_err( 'unrecognized_parm', p_statistics );
      END CASE;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      p_partname       VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_queue_module   VARCHAR2 DEFAULT NULL,
      p_queue_action   VARCHAR2 DEFAULT NULL
   )
   IS
      l_ddl             LONG;
      l_idx_cnt         NUMBER                                       := 0;
      l_tab_name        VARCHAR2( 61 )                               := UPPER( p_owner || '.' || p_table );
      l_src_name        VARCHAR2( 61 )                              := UPPER( p_source_owner || '.' || p_source_table );
      l_part_type       VARCHAR2( 6 );
      l_targ_part       all_tables.partitioned%TYPE;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_concurrent_id   VARCHAR2( 100 );
      l_rows            BOOLEAN                                      := FALSE;
      e_dup_idx_name    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_idx_name, -955 );
      e_dup_col_list    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_col_list, -1408 );
      o_ev              evolve_ot                                    := evolve_ot( p_module => 'build_indexes' );
   BEGIN
      -- confirm that parameters are compatible
      -- go ahead and write a CASE statement so adding more later is easier
      CASE
         WHEN p_tablespace IS NOT NULL AND p_partname IS NOT NULL
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible', 'P_TABLESPACE and P_PARTNAME' );
         ELSE
            NULL;
      END CASE;

      -- register the value of p_concurrent
      evolve.log_msg( 'The value of P_CONCURRENT is: '||p_concurrent, 5 );

      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table, p_partname => p_partname );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE );
      -- we need the segment attributes so things go where we want them to
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', TRUE );
      -- don't want all the other storage aspects though
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE', FALSE );
      o_ev.change_action( 'build indexes' );

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

      -- need to get a unique "job header" number in case we are running concurrently
      IF td_core.is_true( p_concurrent )
      THEN
	 o_ev.change_action( 'get concurrent id' );
         l_concurrent_id    := evolve.get_concurrent_id;
      END IF;

      -- create a cursor containing the DDL from the target indexes
      FOR c_indexes IN
         ( SELECT UPPER( p_owner ) index_owner, new_index_name index_name, owner source_owner, index_name source_index,
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
                  'Index ' || owner || '.' || new_index_name || ' renamed to ' || index_name rename_msg
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
                                  UPPER(    SUBSTR( p_table, 1, 24 )
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
                                                                  '(\.)("?)(' || p_source_table || ')("?)(\s+)(\()',
                                                                  '\1' || UPPER( p_table ) || '\5\6',
                                                                  1,
                                                                  0,
                                                                  'i'
                                                                ),
                                                  '(")?(' || ind.owner || ')("?\.)',
                                                  UPPER( p_owner ) || '.',
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
                                                  -- tablespace desisions are based on the p_TABLESPACE parameter
                                                  -- partitioning decisions are based on the structure of the target table
                                                  CASE
                                                     -- target is not partitioned and neither p_TABLESPACE or p_PARTNAME are provided
                                                  WHEN l_targ_part = 'NO' AND p_tablespace IS NULL
                                                       AND p_partname IS NULL
                                                        -- remove all partitioning and the local keyword
                                                  THEN '\s*(\(\s*partition.+\))|local\s*'
                                                     -- target is not partitioned but p_TABLESPACE or p_PARTNAME is provided
                                                  WHEN l_targ_part = 'NO'
                                                  AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                        -- strip out partitioned info and local keyword and tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned and p_TABLESPACE or p_PARTNAME is provided
                                                  WHEN l_targ_part = 'YES'
                                                  AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                        -- strip out partitioned info keeping local keyword and remove tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned
                                                     -- p_TABLESPACE is null
                                                     -- p_PARTNAME is null
                                                  WHEN l_targ_part = 'YES' AND p_tablespace IS NULL
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
                                                -- if p_TABLESPACE is provided, then previous tablespace information was stripped (above)
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
                                                                    AND partition_position = l_part_position )
                                                              )
                                                ELSE NULL
                                             END index_ddl,
                                          table_owner, table_name, owner, index_name,
                                          
                                          -- this is the index name that will be used in the first attempt
                                          -- basically, all cases of the previous table name are replaced with the new table name
                                          UPPER( REGEXP_REPLACE( index_name,
                                                                 '(")?' || p_source_table || '(")?',
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
                                                         WHEN REGEXP_LIKE( 'global', p_part_type, 'i' )
                                                            THEN 'NO'
                                                         WHEN REGEXP_LIKE( 'local', p_part_type, 'i' )
                                                            THEN 'YES'
                                                         ELSE '.'
                                                      END,
                                                      'i'
                                                    )
                                     AND table_name = UPPER( p_source_table )
                                     AND table_owner = UPPER( p_source_owner )
                                     -- iot indexes provide a problem when in CONCURRENT mode
                                     -- the code just handles the errors with exceptions
                                     -- but CONCURRENT processes are subject to exceptions in the flow of the program
                                     -- so we just don't support certain paradigms in concurrent mode
                                     -- one of them is building having a mismatch between table types when considering IOT's
                                     AND index_type <>
                                                  CASE td_core.get_yn_ind( p_concurrent )
                                                     WHEN 'yes'
                                                        THEN 'IOT - TOP'
                                                     ELSE '~'
                                                  END
                                     -- USE an NVL'd regular expression to determine the specific indexes to work on
                                     -- when nothing is passed for p_INDEX_TYPE, then that is the same as passing a wildcard
                                     AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
                                     -- USE an NVL'd regular expression to determine the index types to worked on
                                     -- when nothing is passed for p_INDEX_TYPE, then that is the same as passing a wildcard
                                     AND REGEXP_LIKE( index_type, '^' || NVL( p_index_type, '.' ), 'i' )) ind
                                 LEFT JOIN
                                 ( SELECT index_name index_name_confirm, owner index_owner_confirm
                                    FROM all_indexes ) aii
                                 ON aii.index_name_confirm = ind.idx_rename
                               AND aii.index_owner_confirm = UPPER( p_owner )
                                 )))
      LOOP
         l_rows    := TRUE;

         BEGIN
            o_ev.change_action( 'execute index DDL' );
            evolve.exec_sql( p_sql => c_indexes.index_ddl, p_auto => 'yes', p_concurrent_id => l_concurrent_id );
            evolve.log_msg(    'Index '
                                || c_indexes.index_name
                                || ' '
                                || CASE
                                      WHEN td_core.is_true( p_concurrent )
                                         THEN 'creation submitted to the Oracle scheduler'
                                      ELSE 'built'
                                   END,
                                2
                              );
            l_idx_cnt    := l_idx_cnt + 1;
            o_ev.change_action( 'enqueue idx rename DDL' );

            -- queue up alternative DDL statements for later use
            -- in this case, queue up index rename statements
            -- these statements are used by module 'replace_table' and action 'rename indexes'
            IF p_queue_module = 'replace_table' AND p_queue_action = 'rename indexes'
            THEN
	       enqueue_ddl( p_stmt     => c_indexes.rename_ddl,
			    p_msg      => c_indexes.rename_msg,
			    p_module   => p_queue_module,
			    p_action   => p_queue_action );
            END IF;
         EXCEPTION
            -- if a duplicate column list of indexes already exist, log it, but continue
            WHEN e_dup_col_list
            THEN
               evolve.log_msg( 'Index comparable to ' || c_indexes.source_index || ' already exists', 3 );
            WHEN OTHERS
            THEN
            -- first log the error
            -- provide a backtrace from this exception handler to the next exception
            evolve.log_err;
            RAISE;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No matching indexes found on ' || l_src_name );
      ELSE
         IF td_core.is_true( p_concurrent )
         THEN
            evolve.log_msg( 'P_CONCURRENT is true', 5 );

            IF NOT evolve.is_debugmode
            THEN
               -- now simply waiting for all the concurrent processes to complete
               o_ev.change_action( 'wait on concurrent processes' );
               evolve.coordinate_sql( p_concurrent_id => l_concurrent_id, p_raise_err => 'no' );
            END IF;
         END IF;

         evolve.log_msg(    l_idx_cnt
                             || ' index creation process'
                             || CASE
                                   WHEN l_idx_cnt = 1
                                      THEN NULL
                                   ELSE 'es'
                                END
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'executed'
                                END
                             || ' for '
                             || l_tab_name
                           );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END build_indexes;

   -- builds the constraints from one table on another
   PROCEDURE build_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table',
      p_seg_attributes      VARCHAR2 DEFAULT 'no',
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_queue_module        VARCHAR2 DEFAULT NULL,
      p_queue_action   	    VARCHAR2 DEFAULT NULL
   )
   IS
      l_targ_part       all_tables.partitioned%TYPE;
      l_iot_type        all_tables.iot_type%TYPE;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_con_cnt         NUMBER                                       := 0;
      l_tab_name        VARCHAR2( 61 )                               := UPPER( p_owner || '.' || p_table );
      l_src_name        VARCHAR2( 61 )                              := UPPER( p_source_owner || '.' || p_source_table );
      l_concurrent_id   VARCHAR2(100);
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
      o_ev              evolve_ot                                    := evolve_ot( p_module => 'build_constraints' );
   BEGIN
      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table, p_partname => p_partname );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE );
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

      -- get whether the table is a IOT
      -- need to know this because that means that the primary key constraint will be included as part of any table DDL
      -- specifically, for the BUILD_TABLE procedure
      -- need to make sure the RENAME_DDL statement makes it into the temporary table even during an exception
      -- also need to know whether the table is partitioned or not
      -- this determines how to build constraints associated with indexes on target table
      SELECT partitioned, iot_type
        INTO l_targ_part, l_iot_type
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

      -- need to get a unique "job header" number in case we are running concurrently
      o_ev.change_action( 'get concurrent id' );

      IF td_core.is_true( p_concurrent )
      THEN
         l_concurrent_id    := evolve.get_concurrent_id;
      END IF;

      o_ev.change_action( 'build constraints' );

      FOR c_constraints IN
         (
-- this case statement uses GENERIC_CON column to determine the final index name
-- GENERIC_CON is a case statement that is generated below
-- IF we are using a generic name, then perform the replace
          SELECT   constraint_owner, CASE generic_con
                      WHEN 'Y'
                         THEN con_rename_adj
                      ELSE con_rename
                   END constraint_name, source_owner, source_table, source_constraint, constraint_type, index_owner,
                   index_name,
                   REGEXP_REPLACE( constraint_ddl,
                                   '(constraint )("?)(\w+)("?)',
                                   '\1' || CASE generic_con
                                      WHEN 'Y'
                                         THEN con_rename_adj
                                      ELSE con_rename
                                   END,
                                   1,
                                   0,
                                   'i'
                                 ) constraint_ddl,
                      
                      -- this column was added for the REPLACE_TABLE procedure
                      -- IN that procedure, after cloning the indexes, the table is renamed
                      -- we have to rename the indexes back to their original names
                      ' alter table '
                   || source_owner
                   || '.'
                   || source_table
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
                   || source_owner
                   || '.'
                   || source_table
                   || ' renamed to '
                   || source_constraint rename_msg,
                   basis_source, generic_con, named_constraint
              FROM ( SELECT
                            -- IF con_rename already exists (constructed below), then we will try to rename the constraint to something generic
                            -- this name will only be used when con_rename name already exists
                            UPPER(    CASE basis_source
                                         WHEN 'reference'
                                            THEN 'TD$_CON' || TO_CHAR( SYSTIMESTAMP, 'mmddyyyyHHMISS' )
                                         ELSE SUBSTR( p_table, 1, 24 ) || '_' || con_ext
                                      END
                                   || CASE constraint_type
                                         WHEN 'P'
                                            THEN NULL
                                         ELSE RANK( ) OVER( PARTITION BY con_ext ORDER BY constraint_name )
                                      END
                                 
                                 -- rank function gives us the constraint number by specific constraint extension (formulated below)
                                 ) con_rename_adj,
                            
                                      -- this regexp_replace replaces the current owner of the table with the new owner of the table
                            -- when p_BASIS is 'table', then it replaces the owner for the table being altered
                            -- when p_BASIS is 'reference', then it replaces it for the table being referenced
                            -- the CASE statement looks for either 'TABLE' or 'REFERENCES' before the owner name
                            REGEXP_REPLACE(
                                            -- this regexp_replace will replace the source table with the target table
                                            -- this only has an effect when p_BASIS is 'table'
                                            -- when p_BASIS is 'reference', we are gauranteed to have a match for p_SOURCE_TABLE
                                            -- that's because this constraint was found because it references p_SOURCE_TABLE
                                            -- a reference cannot be to itself
                                            REGEXP_REPLACE(
                                                            -- this regexp_replace simply removes any "ATLER CONSTRAINT..." commands that might be in here
                                                            -- DBMS_METADATA can append ALTER CONSTRAINT commands after the ADD CONSTRAINT commands
                                                            -- we don't want that
                                                            REGEXP_REPLACE( constraint_ddl,
                                                                            '(alter constraint).+',
                                                                            NULL,
                                                                            1,
                                                                            0,
                                                                            'i'
                                                                          ),
                                                            '(\.)("?)(' || p_source_table || ')(\w*)("?)',
                                                            '\1' || UPPER( p_table ) || '\4',
                                                            1,
                                                            0,
                                                            'i'
                                                          ),
                                               '('
                                            || CASE basis_source
                                                  WHEN 'table'
                                                     THEN 'table'
                                                  ELSE 'references'
                                               END
                                            || ' )(")?('
                                            || con.owner
                                            || ')("?\.)',
                                            '\1' || UPPER( p_owner ) || '.',
                                            1,
                                            0,
                                            'i'
                                          ) constraint_ddl,
                            con.owner source_owner, table_name source_table, constraint_name source_constraint,
                            con_rename, index_owner, index_name, con_ext, constraint_type,
                            
                            -- this case expression determines whether to use the standard renamed constraint name
                            -- OR whether to use the generic constraint name based on table name
                            -- below we are right joining with USER_OBJECTS to see if the standard name is already used
                            -- IF we match, then we need to use the generic constraint name
                            CASE
                               WHEN( constraint_name_confirm IS NULL AND LENGTH( con_rename ) < 31 )
                                  THEN 'N'
                               ELSE 'Y'
                            END generic_con,
                            basis_source, constraint_owner,
                            CASE
                               WHEN TO_CHAR( REGEXP_SUBSTR( constraint_ddl, 'constraint', 1, 1, 'i' )) IS NULL
                                  THEN 'N'
                               ELSE 'Y'
                            END named_constraint
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
                                            -- TABLESPACE desisions are based on the p_TABLESPACE parameter
                                            -- partitioning decisions are based on the structure of the target table
                                            CASE
                                               -- target is not partitioned and neither p_TABLESPACE or p_PARTNAME are provided
                                            WHEN l_targ_part = 'NO' AND p_tablespace IS NULL AND p_partname IS NULL
                                                  -- remove all partitioning and the local keyword
                                            THEN '\s*(\(\s*partition.+\))|local\s*'
                                               -- target is not partitioned but p_TABLESPACE or p_PARTNAME is provided
                                            WHEN l_targ_part = 'NO'
                                            AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                  -- strip out partitioned info and local keyword and tablespace clause
                                            THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                               -- target is partitioned and p_TABLESPACE or p_PARTNAME is provided
                                            WHEN l_targ_part = 'YES'
                                            AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                  -- strip out partitioned info keeping local keyword and remove tablespace clause
                                            THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                               -- target is partitioned
                                               -- p_tablespace IS NULL
                                               -- p_partname IS NULL
                                            WHEN l_targ_part = 'YES' AND p_tablespace IS NULL AND p_partname IS NULL
                                                  -- leave partitioning and tablespace information as it is
                                                  -- this implies a one-to-one mapping of partitioned names from source to target
                                            THEN NULL
                                            END,
                                            ' ',
                                            1,
                                            0,
                                            'in'
                                          )
                                    -- this case statement will append tablespace information on the end where applicable
                                    -- anytime a value is passed for p_TABLESPACE, then other tablespace information is stripped off
                                    || CASE
                                          -- if the INDEX_NAME column is null, then there is no index associated with this constraint
                                          -- that means that tablespace information would be meaningless.
                                          -- also, IF 'default' is passed, then use the users default tablespace
                                       WHEN ac.index_name IS NULL OR LOWER( p_tablespace ) = 'default'
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
                                                              AND partition_position = l_part_position )
                                                        )
                                          ELSE NULL
                                       END constraint_ddl,
                                    ac.owner, ac.table_name, ac.constraint_name, ac.index_owner, ac.index_name,
                                    
                                    -- this is the constraint name that will be used if it doesn't already exist
                                    -- basically, all cases of the previous table name are replaced with the new table name
                                    UPPER( REGEXP_REPLACE( constraint_name,
                                                           '(")?' || p_source_table || '(")?',
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
                                    END con_ext, constraint_type, basis_source,
                                    CASE basis_source
                                       WHEN 'reference'
                                          THEN ac.owner
                                       ELSE UPPER( p_owner )
                                    END constraint_owner
                              FROM ( SELECT *
                                      FROM ( SELECT ac.*,
                                                    CASE
                                                       WHEN REGEXP_LIKE( 'table|all', p_basis, 'i' )
                                                          THEN 'Y'
                                                       ELSE 'N'
                                                    END include,
                                                    'table' basis_source
                                              FROM all_constraints ac
                                             WHERE ac.table_name = UPPER( p_source_table )
                                               AND ac.owner = UPPER( p_source_owner )
                                               AND REGEXP_LIKE( constraint_type, NVL( p_constraint_type, '.' ), 'i' )
                                            UNION ALL
                                            SELECT ac.*,
                                                   CASE
                                                      WHEN REGEXP_LIKE( 'reference|all', p_basis, 'i' )
                                                         THEN 'Y'
                                                      ELSE 'N'
                                                   END include,
                                                   'reference' basis_source
                                              FROM all_constraints ac
                                             WHERE constraint_type = 'R'
                                               AND r_constraint_name IN(
                                                      SELECT constraint_name
                                                        FROM all_constraints
                                                       WHERE table_name = UPPER( p_source_table )
                                                         AND owner = UPPER( p_source_owner )
                                                         AND constraint_type = 'P' ))
                                     WHERE REGEXP_LIKE( constraint_name, NVL( p_constraint_regexp, '.' ), 'i' )
                                       AND include = 'Y' ) ac
                                   LEFT JOIN
                                   all_indexes ai ON ac.index_owner = ai.owner AND ac.index_name = ai.index_name
                                   ) con
                           LEFT JOIN
                           ( SELECT constraint_name constraint_name_confirm, owner constraint_owner_confirm
                              FROM all_constraints ) acc
                           ON acc.constraint_name_confirm = con.con_rename
                         AND acc.constraint_owner_confirm = constraint_owner
                           )
          ORDER BY basis_source DESC )
      LOOP
         -- catch empty cursor sets
         l_rows    := TRUE;

         -- if the target table is index-organized and the constraint is a primary key
         -- then we don't want to build the constraint
         IF NOT( td_utils.is_iot( p_owner, p_table ) AND c_constraints.constraint_type = 'P' )
         THEN
            BEGIN
               evolve.exec_sql( p_sql                => c_constraints.constraint_ddl,
                                    p_auto               => 'yes',
                                    p_concurrent_id      => l_concurrent_id
                                  );
               evolve.log_msg(    'Creation of '
                                   || CASE c_constraints.named_constraint
                                         WHEN 'Y'
                                            THEN 'constraint ' || c_constraints.constraint_name
                                         ELSE 'unnamed constraint'
                                      END
                                   || ' '
                                   || CASE
                                         WHEN td_core.is_true( p_concurrent )
                                            THEN 'submitted to the Oracle scheduler'
                                         ELSE 'executed'
                                      END,
                                   2
                                 );
               l_con_cnt    := l_con_cnt + 1;
               o_ev.change_action( 'enqueue build idx DDL' );

               -- only insert for rename if the constraint is a named constraint
               -- queue up alternative DDL statements for later use
               -- in this case, queue up constraint rename statements
               -- these statements are used by module 'replace_table' and action 'rename constraints'
               IF c_constraints.named_constraint = 'Y' AND p_queue_module = 'replace_table' AND p_queue_action = 'rename constraints'
               THEN
		  enqueue_ddl( p_stmt	     => c_constraints.rename_ddl,
			       p_msg  	     => c_constraints.rename_msg,
			       p_module	     => p_queue_module,
			       p_action	     => p_queue_action );

               END IF;
            EXCEPTION
               WHEN e_dup_pk
               THEN
                  evolve.log_msg( 'Primary key constraint already exists on table ' || l_tab_name, 3 );
               WHEN e_dup_fk
               THEN
                  evolve.log_msg(    'Constraint comparable to '
                                      || c_constraints.constraint_name
                                      || ' already exists on table '
                                      || l_tab_name,
                                      3
                                    );
               WHEN e_dup_not_null
               THEN
                  evolve.log_msg( 'Referenced not null constraint already exists on table ' || l_tab_name, 3 );
               WHEN OTHERS
               THEN
                  -- first log the error
                  -- provide a backtrace from this exception handler to the next exception
                  evolve.log_err;
                  o_ev.clear_app_info;
                  RAISE;
            END;
         END IF;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No matching constraints found on ' || l_src_name );
      ELSE
         IF td_core.is_true( p_concurrent )
         THEN
            IF NOT evolve.is_debugmode
            THEN
               -- now simply waiting for all the concurrent processes to complete
               o_ev.change_action( 'wait on concurrent processes' );
               evolve.coordinate_sql( p_concurrent_id => l_concurrent_id, p_raise_err => 'no' );
            END IF;
         END IF;

         evolve.log_msg(    l_con_cnt
                             || ' constraint'
                             || CASE
                                   WHEN l_con_cnt = 1
                                      THEN NULL
                                   ELSE 's'
                                END
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'built'
                                END
                             || ' for '
                             || l_tab_name
                           );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END build_constraints;

   -- disables constraints related to a particular table
   PROCEDURE constraint_maint(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_maint_type          VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_queue_module        VARCHAR2 DEFAULT NULL,
      p_queue_action        VARCHAR2 DEFAULT NULL
   )
   IS
      l_con_cnt         NUMBER         := 0;
      l_tab_name        VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_concurrent_id   VARCHAR2( 100 );
      l_rows_num        NUMBER;
      l_rows            BOOLEAN        := FALSE;
      e_iot_shc         EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_iot_shc, -25188 );
      o_ev              evolve_ot      := evolve_ot( p_module => 'constraint_maint' );
   BEGIN
      -- P_CONSTRAINT_TYPE only relates to constraints based on the table, not the reference
      IF REGEXP_LIKE( 'reference|all', p_basis, 'i' ) AND p_constraint_type IS NOT NULL
      THEN
         evolve.log_msg( 'A value provided in P_CONSTRAINT_TYPE is ignored for constraints based on references' );
      END IF;

      -- confirm that the table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- need to get a unique "job header" number in case we are running concurrently
      o_ev.change_action( 'get concurrent id' );

      IF td_core.is_true( p_concurrent )
      THEN
         l_concurrent_id    := evolve.get_concurrent_id;
      END IF;

      -- disable both table and reference constraints for this particular table
      o_ev.change_action( 'constraint maintenance' );

      FOR c_constraints IN ( SELECT  *
                                FROM ( SELECT
                                              -- need this to get the order by clause right
                                              -- when we are disabling, we need references to go first
                                              -- when we are enabling, we need referenced (primary keys) to go first
                                              CASE LOWER( p_maint_type )
                                                 WHEN 'enable'
                                                    THEN 1
                                                 ELSE 2
                                              END ordering, 'table' basis_source, owner table_owner, table_name,
                                              constraint_name,
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
                                              'Constraint ' || constraint_name || ' enabled on '
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
                                         AND REGEXP_LIKE( constraint_name, NVL( p_constraint_regexp, '.' ), 'i' )
                                         AND REGEXP_LIKE( constraint_type, NVL( p_constraint_type, '.' ), 'i' )
                                      UNION
                                      SELECT
                                             -- need this to get the order by clause right
                                             -- when we are disabling, we need references to go first
                                             -- when we are enabling, we need referenced (primary keys) to go first
                                             CASE LOWER( p_maint_type )
                                                WHEN 'enable'
                                                   THEN 2
                                                ELSE 1
                                             END ordering, 'reference' basis_source, owner table_owner, table_name,
                                             constraint_name,
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
                                         AND REGEXP_LIKE( constraint_name, NVL( p_constraint_regexp, '.' ), 'i' )
                                         AND r_constraint_name IN (
                                                SELECT constraint_name
                                                  FROM all_constraints
                                                 WHERE table_name = UPPER( p_table )
                                                   AND owner = UPPER( p_owner )
                                                   AND constraint_type = 'P' )
                                         AND r_owner IN (
                                                SELECT owner
                                                  FROM all_constraints
                                                 WHERE table_name = UPPER( p_table )
                                                   AND owner = UPPER( p_owner )
                                                   AND constraint_type = 'P' ))
                               WHERE include = 'Y'
                            ORDER BY ordering )
      LOOP
         -- catch empty cursor sets
         l_rows    := TRUE;

         BEGIN
            evolve.exec_sql( p_sql                => CASE
                                    WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                       THEN c_constraints.disable_ddl
                                    WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                       THEN c_constraints.enable_ddl
                                 END,
                                 p_auto               => 'yes',
                                 p_concurrent_id      => l_concurrent_id
                               );

            -- queue up alternative DDL statements for later use
            -- this allows a call to DEQUEUE_DDL to only work on those that were previously disabled
            IF REGEXP_LIKE( 'disable', p_maint_type, 'i' )
            THEN
               o_ev.change_action( 'enqueue disable con DDL' );
	       enqueue_ddl( p_stmt	  => c_constraints.disable_ddl,
			    p_msg  	  => c_constraints.disable_msg,
			    p_module	  => p_queue_module,
			    p_action	  => p_queue_action );

            END IF;

            evolve.log_msg( CASE
                                   WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                      THEN c_constraints.disable_msg
                                   WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                      THEN c_constraints.enable_msg
                                END,
                                2
                              );
            l_con_cnt    := l_con_cnt + 1;
         EXCEPTION
            WHEN e_iot_shc
            THEN
               evolve.log_msg(    'Constraint '
                                   || c_constraints.constraint_name
                                   || ' is the primary key for either an IOT or a sorted hash cluster',
                                   3
                                 );
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg(    'No matching '
                             || CASE
                                   WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                      THEN 'enabled'
                                   WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                      THEN 'disabled'
                                END
                             || ' constraints found.'
                           );
      ELSE
         -- wait for the concurrent processes to complete or fail
         IF td_core.is_true( p_concurrent ) AND NOT evolve.is_debugmode
         THEN
            evolve.coordinate_sql( p_concurrent_id => l_concurrent_id, p_raise_err => 'no' );
         END IF;

         evolve.log_msg( 'Value for P_MAINT_TYPE: ' || p_maint_type, 5 );
         evolve.log_msg(    l_con_cnt
                             || ' constraint '
                             || CASE
                                   WHEN REGEXP_LIKE( 'disable', p_maint_type, 'i' )
                                      THEN 'disablement'
                                   WHEN REGEXP_LIKE( 'enable', p_maint_type, 'i' )
                                      THEN 'enablement'
                                END
                             || ' process'
                             || CASE
                                   WHEN l_con_cnt = 1
                                      THEN NULL
                                   ELSE 'es'
                                END
                             || ' '
                             || CASE
                                   WHEN REGEXP_LIKE( 'table', p_basis, 'i' )
                                      THEN 'for'
                                   WHEN REGEXP_LIKE( 'reference', p_basis, 'i' )
                                      THEN 'related to'
                                   ELSE 'involving'
                                END
                             || ' '
                             || l_tab_name
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'executed'
                                END
                           );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END constraint_maint;

   -- drop particular indexes from a table
   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL
   )
   IS
      l_rows       BOOLEAN        := FALSE;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_idx_cnt    NUMBER         := 0;
      e_pk_idx     EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_pk_idx, -2429 );
      o_ev         evolve_ot      := evolve_ot( p_module => 'drop_indexes' );
   BEGIN
      FOR c_indexes IN ( SELECT 'drop index ' || owner || '.' || index_name index_ddl, index_name, table_name, owner,
                                owner || '.' || index_name full_index_name
                          FROM all_indexes
                         WHERE table_name = UPPER( p_table )
                           AND table_owner = UPPER( p_owner )
                           AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
                           AND REGEXP_LIKE( index_type, '^' || NVL( p_index_type, '.' ), 'i' )
                           AND REGEXP_LIKE( partitioned,
                                            CASE
                                               WHEN REGEXP_LIKE( 'global', p_part_type, 'i' )
                                                  THEN 'NO'
                                               WHEN REGEXP_LIKE( 'local', p_part_type, 'i' )
                                                  THEN 'YES'
                                               ELSE '.'
                                            END,
                                            'i'
                                          ))
      LOOP
         l_rows    := TRUE;

         BEGIN
            evolve.exec_sql( p_sql => c_indexes.index_ddl, p_auto => 'yes' );
            l_idx_cnt    := l_idx_cnt + 1;
            evolve.log_msg( 'Index ' || c_indexes.index_name || ' dropped', 3 );
         EXCEPTION
            WHEN e_pk_idx
            THEN
               NULL;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No matching indexes to drop found on ' || l_tab_name );
      ELSE
         evolve.log_msg( l_idx_cnt || ' index' || CASE
                                WHEN l_idx_cnt = 1
                                   THEN NULL
                                ELSE 'es'
                             END || ' dropped on ' || l_tab_name
                           );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END drop_indexes;

   -- drop particular constraints from a table
   PROCEDURE drop_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   )
   IS
      l_con_cnt    NUMBER         := 0;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_rows       BOOLEAN        := FALSE;
      l_iot_pk     BOOLEAN        := FALSE;
      e_iot_pk     EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_iot_pk, -25188 );
      o_ev         evolve_ot      := evolve_ot( p_module => 'drop_constraints' );
   BEGIN
      -- confirm that the table exists
      -- raise an error if it doesn't
      td_utils.check_table( p_owner => p_owner, p_table => p_table );

      -- drop constraints
      FOR c_constraints IN ( SELECT  *
                                FROM ( SELECT    'alter table '
                                              || owner
                                              || '.'
                                              || table_name
                                              || ' drop constraint '
                                              || constraint_name constraint_ddl,
                                              constraint_name, table_name,
                                              CASE
                                                 WHEN REGEXP_LIKE( 'table|all', p_basis, 'i' )
                                                    THEN 'Y'
                                                 ELSE 'N'
                                              END include, 'table' basis_source
                                        FROM all_constraints
                                       WHERE table_name = UPPER( p_table )
                                         AND owner = UPPER( p_owner )
                                         AND REGEXP_LIKE( constraint_name, NVL( p_constraint_regexp, '.' ), 'i' )
                                         AND REGEXP_LIKE( constraint_type, NVL( p_constraint_type, '.' ), 'i' )
                                      UNION
                                      SELECT    'alter table '
                                             || owner
                                             || '.'
                                             || table_name
                                             || ' drop constraint '
                                             || constraint_name constraint_ddl,
                                             constraint_name, table_name,
                                             CASE
                                                WHEN REGEXP_LIKE( 'reference|all', p_basis, 'i' )
                                                   THEN 'Y'
                                                ELSE 'N'
                                             END include, 'reference' basis_source
                                        FROM all_constraints
                                       WHERE constraint_type = 'R'
                                         AND REGEXP_LIKE( constraint_name, NVL( p_constraint_regexp, '.' ), 'i' )
                                         AND r_constraint_name IN(
                                                SELECT constraint_name
                                                  FROM all_constraints
                                                 WHERE table_name = UPPER( p_table )
                                                   AND owner = UPPER( p_owner )
                                                   AND constraint_type = 'P' ))
                               WHERE include = 'Y'
                            ORDER BY basis_source )
      LOOP
         -- catch empty cursor sets
         l_rows    := TRUE;

         BEGIN
            o_ev.change_action( 'execute table alter' );
            evolve.exec_sql( p_sql => c_constraints.constraint_ddl, p_auto => 'yes' );
            l_con_cnt    := l_con_cnt + 1;
            evolve.log_msg( 'Constraint ' || c_constraints.constraint_name || ' dropped', 2 );
         EXCEPTION
            WHEN e_iot_pk
            THEN
               evolve.log_msg(    c_constraints.constraint_name
                                   || ' cannot be dropped because it is the primary key of an iot or cluster',
                                   3
                                 );
               l_iot_pk    := TRUE;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No matching constraints to drop found on ' || l_tab_name );
      ELSE
         evolve.log_msg(    l_con_cnt
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

      IF l_iot_pk
      THEN
	 o_ev.clear_app_info;
         RAISE drop_iot_key;
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      l_none_msg    VARCHAR2( 100 ) := 'No matching object privileges found on ' || l_src_name;
      l_grants      BOOLEAN         := TRUE;
      e_no_grants   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grants, -31608 );
      o_ev          evolve_ot       := evolve_ot( p_module => 'object_grants' );
   BEGIN
      -- confirm that the target table exists
      -- raise an error if it doesn't
      td_utils.check_object( p_owner => p_owner, p_object => p_object );
      -- confirm that the source table
      -- raise an error if it doesn't
      td_utils.check_object( p_owner => p_source_owner, p_object => p_source_object );
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE );
      o_ev.change_action( 'extract grants' );
      -- we need the sql terminator now because it will be our split character later
      DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'SQLTERMINATOR', TRUE );

      -- create a cursor containing the DDL from the target indexes
      BEGIN
         -- need to remove the last sql terminator because it's a splitter between statements
         -- also remove all the extract spaces and carriage returns
         SELECT REGEXP_REPLACE( REGEXP_REPLACE( DDL, ' *\s+ +', NULL ), ';\s*$', NULL )
           INTO l_ddl
           FROM ( SELECT ( REGEXP_REPLACE( REGEXP_REPLACE( DBMS_METADATA.get_dependent_ddl( 'OBJECT_GRANT',
                                                                                            object_name,
                                                                                            owner
                                                                                          ),
                                                           '(\."?)(' || UPPER( p_source_object ) || ')(\w*)("?)',
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
            l_grants    := FALSE;
            evolve.log_msg( l_none_msg, 3 );
      END;

      -- now, parse the string to work on the different values in it
      o_ev.change_action( 'execute grants' );

      FOR c_grants IN ( SELECT *
                         FROM TABLE( td_core.SPLIT( l_ddl, ';' )))
      LOOP
         IF l_grants
         THEN
            evolve.exec_sql( p_sql => c_grants.COLUMN_VALUE, p_auto => 'yes' );
         END IF;

         l_grant_cnt    := l_grant_cnt + 1;
      END LOOP;

      IF l_grants
      THEN
         evolve.log_msg(    l_grant_cnt
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
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      o_ev         evolve_ot      := evolve_ot( p_module => 'insert_table', p_action => 'Check existence of objects' );
   BEGIN
      -- check information about the table
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- check that the source object exists.
      td_utils.check_object( p_owner => p_source_owner, p_object => p_source_object, p_object_type => 'table|view|synonym' );
      o_ev.change_action( 'issue log_errors warning' );

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF td_core.is_true( p_direct ) AND p_log_table IS NOT NULL
      THEN
         evolve.log_msg
                 ( 'Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
                   3
                 );
      END IF;

      o_ev.change_action( 'truncate table' );

      IF td_core.is_true( p_trunc )
      THEN
         -- truncate the target table
         truncate_table( p_owner, p_table );
      END IF;

      -- enable|disable parallel dml depending on the parameter for P_DIRECT
      o_ev.change_action( 'alter parallel dml' );
      evolve.exec_sql(    'ALTER SESSION '
                           || CASE
                                 WHEN REGEXP_LIKE( 'yes', p_direct, 'i' )
                                    THEN 'ENABLE'
                                 ELSE 'DISABLE'
                              END
                           || ' PARALLEL DML'
                         );
      o_ev.change_action( 'issue insert statement' );
      evolve.exec_sql( p_sql      =>    'insert '
                                         || CASE
                                               WHEN td_core.is_true( p_direct )
                                                  THEN '/*+ APPEND */ '
                                               ELSE NULL
                                            END
                                         || 'into '
                                         || l_trg_name
                                         || ' select '
                                         || CASE
                                               -- just use a regular expression to remove the APPEND hint if P_DIRECT is disabled
                                            WHEN p_degree IS NOT NULL
                                                  THEN '/*+ PARALLEL (source ' || p_degree || ') */ '
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
      IF NOT evolve.is_debugmode
      THEN
         evolve.log_cnt_msg( p_count      => SQL%ROWCOUNT,
                                 p_msg        => 'Number of records inserted into ' || l_trg_name );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      o_ev              evolve_ot   := evolve_ot( p_module      => 'merge_table',
                                                  p_action      => 'Check existence of objects' );
   BEGIN
      -- check information about the table
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- check that the source object exists.
      td_utils.check_object( p_owner => p_source_owner, p_object => p_source_object, p_object_type => 'table$|view' );
      o_ev.change_action( 'issue log_errors warning' );

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF REGEXP_LIKE( 'yes', p_direct, 'i' ) AND p_log_table IS NOT NULL
      THEN
         evolve.log_msg
                 ( 'Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
                   3
                 );
      END IF;

      o_ev.change_action( 'construct merge on clause' );

      -- use the columns provided in P_COLUMNS.
      -- if that is left null, then choose the columns in the primary key of the target table
      -- if there is no primary key, then choose a unique key (any unique key)
      IF p_columns IS NOT NULL
      THEN
         WITH DATA AS
              
              -- this allows us to create a variable IN LIST based on multiple column names provided
              ( SELECT    TRIM( SUBSTR( COLUMNS,
                                        INSTR( COLUMNS, ',', 1, LEVEL ) + 1,
                                        INSTR( COLUMNS, ',', 1, LEVEL + 1 ) - INSTR( COLUMNS, ',', 1, LEVEL ) - 1
                                      )
                              ) AS token
                     FROM ( SELECT ',' || UPPER( p_columns ) || ',' COLUMNS
                             FROM DUAL )
               CONNECT BY LEVEL <= LENGTH( UPPER( p_columns )) - LENGTH( REPLACE( UPPER( p_columns ), ',', '' )) + 1 )
         SELECT REGEXP_REPLACE( '(' || stragg( 'target.' || column_name || ' = source.' || column_name ) || ')',
                                ',',
                                ' AND' || CHR( 10 )
                              ) LIST
           INTO l_onclause
           FROM all_tab_columns
          WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
                                                                          -- select from the variable IN LIST
                AND column_name IN( SELECT *
                                     FROM DATA );
      ELSE
         -- otherwise, we need to get a constraint name
         -- we first choose a PK if it exists
         -- otherwise get a UK at random
         SELECT LIST
           INTO l_onclause
           FROM ( SELECT REGEXP_REPLACE( '(' || stragg( 'target.' || column_name || ' = source.' || column_name ) || ')',
                                         ',',
                                         ' AND' || CHR( 10 )
                                       ) LIST,
                         
                         -- the MIN function will ensure that primary keys are selected first
                         -- otherwise, it will randonmly choose a remaining constraint to use
                         MIN( dc.constraint_type ) con_type
                   FROM all_cons_columns dcc JOIN all_constraints dc USING( constraint_name, table_name )
                  WHERE table_name = UPPER( p_table )
                    AND dcc.owner = UPPER( p_owner )
                    AND dc.constraint_type IN( 'P', 'U' ));
      END IF;

      o_ev.change_action( 'construct merge update clause' );

      IF p_columns IS NOT NULL
      THEN
         SELECT REGEXP_REPLACE( stragg( 'target.' || column_name || ' = source.' || column_name ), ',',
                                ',' || CHR( 10 ))
           INTO l_update
           -- if P_COLUMNS is provided, we use the same logic from the ON clause
           -- to make sure those same columns are not inlcuded in the update clause
           -- MINUS gives us that
         FROM   ( WITH DATA AS
                       ( SELECT    TRIM( SUBSTR( COLUMNS,
                                                 INSTR( COLUMNS, ',', 1, LEVEL ) + 1,
                                                 INSTR( COLUMNS, ',', 1, LEVEL + 1 ) - INSTR( COLUMNS, ',', 1, LEVEL )
                                                 - 1
                                               )
                                       ) AS token
                              FROM ( SELECT ',' || UPPER( p_columns ) || ',' COLUMNS
                                      FROM DUAL )
                        CONNECT BY LEVEL <=
                                         LENGTH( UPPER( p_columns )) - LENGTH( REPLACE( UPPER( p_columns ), ',', '' ))
                                         + 1 )
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
                 MINUS
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner ) AND column_name IN( SELECT *
                                                                                                        FROM DATA ));
      ELSE
         -- otherwise, we once again MIN a constraint type to ensure it's the same constraint
         -- then, we just minus the column names so they aren't included
         SELECT REGEXP_REPLACE( stragg( 'target.' || column_name || ' = source.' || column_name ), ',',
                                ',' || CHR( 10 ))
           INTO l_update
           FROM ( SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
                 MINUS
                 SELECT column_name
                   FROM ( SELECT  column_name, MIN( dc.constraint_type ) con_type
                             FROM all_cons_columns dcc JOIN all_constraints dc USING( constraint_name, table_name )
                            WHERE table_name = UPPER( p_table )
                              AND dcc.owner = UPPER( p_owner )
                              AND dc.constraint_type IN( 'P', 'U' )
                         GROUP BY column_name ));
      END IF;

      o_ev.change_action( 'construnct merge insert clause' );

      SELECT   REGEXP_REPLACE( '(' || stragg( 'target.' || column_name ) || ') ', ',', ',' || CHR( 10 )) LIST
          INTO l_insert
          FROM all_tab_columns
         WHERE table_name = UPPER( p_table ) AND owner = UPPER( p_owner )
      ORDER BY column_name;

      o_ev.change_action( 'construct merge values clause' );
      l_values    := REGEXP_REPLACE( l_insert, 'target.', 'source.' );

      BEGIN
         o_ev.change_action( 'alter parallel dml' );
         -- ENABLE|DISABLE parallel dml depending on the value of P_DIRECT
         evolve.exec_sql( p_sql      =>    'ALTER SESSION '
                                            || CASE
                                                  WHEN REGEXP_LIKE( 'yes', p_direct, 'i' )
                                                     THEN 'ENABLE'
                                                  ELSE 'DISABLE'
                                               END
                                            || ' PARALLEL DML'
                            );
         o_ev.change_action( 'execute merge statement' );
         -- we put the merge statement together using all the different clauses constructed above
         evolve.exec_sql( p_sql      =>    'MERGE INTO '
                                            || p_owner
                                            || '.'
                                            || p_table
                                            || ' target using '
                                            || CHR( 10 )
                                            || '(select '
                                            || CASE
                                                  -- just use a regular expression to remove the APPEND hint if P_DIRECT is disabled
                                               WHEN p_degree IS NOT NULL
                                                     THEN '/*+ PARALLEL (src ' || p_degree || ') */ '
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
                                                  WHEN td_core.is_true( p_direct )
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
            o_ev.clear_app_info;
            evolve.raise_err( 'on_clause_missing' );
      END;

      -- record the number of rows affected
      IF NOT evolve.is_debugmode
      THEN
         evolve.log_cnt_msg( p_count => SQL%ROWCOUNT, p_msg => 'Number of records merged into ' || l_trg_name );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END merge_table;

   -- queries the dictionary based on regular expressions and loads tables using either the load_tab method or the merge_tab method
   PROCEDURE load_tables(
      p_owner           VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_regexp   VARCHAR2,
      p_suffix          VARCHAR2 DEFAULT NULL,
      p_merge           VARCHAR2 DEFAULT 'no',
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_commit          VARCHAR2 DEFAULT 'yes'
   )
   IS
      l_rows   BOOLEAN   := FALSE;
      o_ev     evolve_ot := evolve_ot( p_module => 'load_tables' );
   BEGIN
      -- dynamic cursor contains source and target objects
      FOR c_objects IN ( SELECT *
			   FROM (SELECT owner source_owner,
					object_name source_object,
					object_type source_object_type,
					upper( regexp_replace( object_name, '(.+)(_)(.+)$', '\1'|| CASE WHEN p_suffix IS NULL THEN NULL ELSE '_'|| p_suffix END )) target_name
				   FROM all_objects 
				  WHERE object_type IN ( 'TABLE', 'VIEW', 'SYNONYM' )) s
			   JOIN ( SELECT owner target_owner,
  					 object_name target_name,
					 object_type target_oject_type
  				    FROM all_objects
				   WHERE object_type IN ( 'TABLE' ) ) t 
				USING (target_name)
			  WHERE REGEXP_LIKE( source_object, p_source_regexp, 'i' )
			    AND source_owner = upper( p_source_owner )
			    AND target_owner = upper( p_owner )
		       )
      LOOP
         l_rows    := TRUE;

         -- use the load_tab or merge_tab procedure depending on P_MERGE
         CASE
            WHEN td_core.is_true( p_merge )
            THEN
               merge_table( p_source_owner       => c_objects.source_owner,
                            p_source_object      => c_objects.source_object,
                            p_owner              => c_objects.target_owner,
                            p_table              => c_objects.target_name,
                            p_direct             => p_direct,
                            p_degree             => p_degree
                          );
            WHEN NOT td_core.is_true( p_merge )
            THEN
               insert_table( p_source_owner       => c_objects.source_owner,
                             p_source_object      => c_objects.source_object,
                             p_owner              => c_objects.target_owner,
                             p_table              => c_objects.target_name,
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
         o_ev.clear_app_info;
         evolve.raise_err( 'incorrect_parameters' );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END load_tables;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_partition(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer',
      p_statpercent    NUMBER DEFAULT NULL,
      p_statdegree     NUMBER DEFAULT NULL,
      p_statmethod     VARCHAR2 DEFAULT NULL
   )
   IS
      l_src_name       VARCHAR2( 61 )                           := UPPER( p_source_owner || '.' || p_source_table );
      l_tab_name       VARCHAR2( 61 )                           := UPPER( p_owner || '.' || p_table );
      l_target_owner   all_tab_partitions.table_name%TYPE       := p_source_owner;
      l_rows           BOOLEAN                                  := FALSE;
      l_partname       all_tab_partitions.partition_name%TYPE;
      l_ddl            LONG;
      l_build_cons     BOOLEAN                                  := FALSE;
      l_compress       BOOLEAN                                  := FALSE;
      l_constraints    BOOLEAN                                  := FALSE;
      l_retry_ddl      BOOLEAN                                  := FALSE;
      e_no_stats       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      e_compress       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_compress, -14646 );
      e_fkeys          EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_fkeys, -2266 );
      e_uk_mismatch    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_uk_mismatch, -14130 );
      e_fk_mismatch    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_fk_mismatch, -14128 );
      o_ev             evolve_ot                                := evolve_ot( p_module => 'exchange_partition' );
   BEGIN
      o_ev.change_action( 'determine partition to use' );
      -- check to make sure the target table exists, is partitioned, and the partition name exists
      td_utils.check_table( p_owner => p_owner, p_table => p_table, p_partname => p_partname, p_partitioned => 'yes' );
      -- check to make sure the source table exists and is not partitioned
      td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table, p_partitioned => 'no' );

      -- use either the value for P_PARTNAME or the max partition
      SELECT NVL( UPPER( p_partname ), partition_name )
        INTO l_partname
        FROM all_tab_partitions
       WHERE table_name = UPPER( p_table )
         AND table_owner = UPPER( p_owner )
         AND partition_position IN( SELECT MAX( partition_position )
                                     FROM all_tab_partitions
                                    WHERE table_name = UPPER( p_table ) AND table_owner = UPPER( p_owner ));

      -- we want to gather statistics
      -- we gather statistics first before the indexes are built
      -- the indexes will collect there own statistics when they are built
      -- that is why we don't cascade
      o_ev.change_action( 'manage statistics' );

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
            o_ev.clear_app_info;
            evolve.raise_err( 'unrecognized_parm', p_statistics );
      END CASE;

      -- now build the indexes
      -- indexes will get fresh new statistics
      -- that is why we didn't mess with these above
      o_ev.change_action( 'build indexes' );
      build_indexes( p_owner             => p_source_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_part_type         => 'local',
                     p_tablespace        => p_index_space,
                     p_concurrent        => p_concurrent,
                     p_partname          => CASE
                        WHEN p_index_space IS NOT NULL
                           THEN NULL
                        ELSE l_partname
                     END
                   );

      -- disable any unique constraints on the target table that are enforced with global indexes
      -- there are multiple reasons for this
      -- first off, there are lots of different errors that can occur because of this situation
      -- it would be difficult to handle and except all of them
      -- the other issue is that this just makes sense: the entire constraint would have to be revalidated anyway
      -- because the index it's based on is updated during the exchange
      o_ev.change_action( 'disable global constraints' );
      FOR c_glob_cons IN ( SELECT *
                             FROM all_constraints ac JOIN all_indexes ai
                                  ON NVL( ac.index_owner, ac.owner ) = ai.owner
                              AND ac.index_name = ai.index_name
                            WHERE constraint_type IN( 'U', 'P' )
                              AND ac.table_name = upper( p_table )
                              AND ac.owner = upper ( p_owner )
                              AND partitioned = 'NO' )
      LOOP
	 evolve.log_msg( 'Unique constraints based on global indexes found ', 5 );
	 l_constraints := TRUE;
         constraint_maint( p_owner                  => p_owner,
                           p_table                  => p_table,
                           p_maint_type             => 'disable',
                           p_constraint_regexp      => '^' || c_glob_cons.constraint_name || '$',
			   p_queue_module	    => evolve.get_module,
                           p_queue_action           => 'enable constraints'
                         );
      END LOOP;


      -- build any constraints on the source table
      o_ev.change_action( 'build constraints' );
      build_constraints( p_owner             => p_source_owner,
                         p_table             => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_table      => p_table,
                         p_concurrent        => p_concurrent
                       );
      -- now exchange the table
      o_ev.change_action( 'exchange table' );

      -- have several exceptions that we want to handle when an exchange fails
      -- so we are using an EXIT WHEN loop
      -- if an exception that we handle is raised, then we want to rerun the exchange
      -- will try the exchange multiple times until it either succeeds, or an unrecognized exception is raised
      LOOP
         l_retry_ddl    := FALSE;

         BEGIN
            evolve.exec_sql( p_sql       =>    'alter table '
                                                || l_tab_name
                                                || ' exchange partition '
                                                || l_partname
                                                || ' with table '
                                                || l_src_name
                                                || ' including indexes without validation update global indexes',
                                 p_auto      => 'yes'
                               );
            evolve.log_msg( l_src_name || ' exchanged for partition ' || l_partname || ' of table ' || l_tab_name );
         EXCEPTION
            WHEN e_fkeys
            THEN
               evolve.log_msg( 'ORA-02266 raised involving enabled foreign keys', 4 );
               -- disable foreign keys related to both tables
               -- this will enable the exchange to occur
               l_constraints    := TRUE;
               l_retry_ddl      := TRUE;
               -- disable foreign keys on the target table
               -- enable them for the queue to be re-enabled later
               o_ev.change_action( 'disable target foreign keys' );
               constraint_maint( p_owner             => p_owner,
                                 p_table             => p_table,
                                 p_maint_type        => 'disable',
                                 p_basis             => 'reference',
				 p_queue_module	     => evolve.get_module,
				 p_queue_action      => 'enable constraints'
                               );
               -- disable constraints related to the source
               -- don't queue enable these, as it will be exchanged in and eventually dropped
               o_ev.change_action( 'disable source foreign keys' );
               constraint_maint( p_owner             => p_source_owner,
                                 p_table             => p_source_table,
                                 p_maint_type        => 'disable',
                                 p_basis             => 'reference'
                               );
            WHEN e_compress
            THEN
               evolve.log_msg( 'ORA-14646 raised involving compression', 4 );
               -- need to compress the staging table
               o_ev.change_action( 'compress source table' );
               l_compress     := TRUE;
               l_retry_ddl    := TRUE;
               evolve.exec_sql( p_sql => 'alter table ' || l_src_name || ' move compress', p_auto => 'yes' );
               evolve.log_msg( l_src_name || ' compressed to facilitate exchange', 3 );
            WHEN e_fk_mismatch
            THEN
               -- need to create foreign key constraints
               evolve.log_msg( 'ORA-14128 raised involving foreign constraint mismatch', 4 );
               -- need to build a foreign keys on the source table
               o_ev.change_action( 'build foreign keys' );
               l_constraints    := TRUE;
               l_retry_ddl      := TRUE;
               build_constraints( p_owner                => p_source_owner,
                                  p_table                => p_source_table,
                                  p_source_owner         => p_owner,
                                  p_source_table         => p_table,
                                  p_constraint_type      => 'r',
                                  p_basis                => 'table',
                                  p_concurrent           => p_concurrent
                                );
            WHEN OTHERS
            THEN
               -- first log the error
               -- provide a backtrace from this exception handler to the next exception
               evolve.log_err;
               -- need to drop indexes if there is an exception
               -- this is for rerunability
               -- now record the reason for the index drops
               evolve.log_msg( 'Dropping indexes for restartability', 3 );
               drop_indexes( p_owner => p_source_owner, p_table => p_source_table );

               -- need to drop constraints if there is an exception
               -- this is for rerunability	    
               -- now record the reason for the index drops
               evolve.log_msg( 'Dropping constraints for restartability', 3 );
               drop_constraints( p_owner => p_source_owner, p_table => p_source_table );

               -- any constraints need to be enabled
               IF l_constraints
               THEN
		  o_ev.change_action( 'enable constraints' );
		  -- this statement will pull previously entered DDL statements off the queue and execute them
		  dequeue_ddl( p_action => evolve.get_action,
			       p_module => evolve.get_module,
			       p_concurrent => p_concurrent );

               END IF;

               o_ev.clear_app_info;
               RAISE;
         END;

         EXIT WHEN NOT l_retry_ddl;
      END LOOP;

      -- any constraints need to be enabled
      IF l_constraints
      THEN
	 o_ev.change_action( 'enable constraints' );
	 -- this statement will pull previously entered DDL statements off the queue and execute them
	 dequeue_ddl( p_action     => evolve.get_action,
		      p_module 	   => evolve.get_module,
		      p_concurrent => p_concurrent);
      END IF;

      -- drop constraints on the stage table
      BEGIN
         drop_constraints( p_owner => p_source_owner, p_table => p_source_table, p_basis => 'all' );
      EXCEPTION
         WHEN drop_iot_key
         THEN
            NULL;
      END;

      -- drop indexes on the staging table
      drop_indexes( p_owner => p_source_owner, p_table => p_source_table );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END exchange_partition;

   -- procedure to "swap" two tables by renaming
   -- this does nothing special... just renames the tables
   -- REPLACE_TABLE does true swapping of two tables, including constraints, indexes, grants
   PROCEDURE rename_tables( p_owner VARCHAR2, p_table VARCHAR2, p_source_table VARCHAR2 )
   IS
      l_src_name     VARCHAR2( 61 )               := UPPER( p_owner || '.' || p_source_table );
      l_tab_name     VARCHAR2( 61 )               := UPPER( p_owner || '.' || p_table );
      l_temp_table   all_tables.table_name%TYPE   := 'TD$_TBL' || TO_CHAR( SYSTIMESTAMP, 'mmddyyyyHHMISS' );
      l_temp_name    VARCHAR2( 61 )               := UPPER( p_owner || '.' || l_temp_table );
      o_ev           evolve_ot                    := evolve_ot( p_module => 'rename_tables' );
   BEGIN
      o_ev.change_action( 'perform object checks' );
      evolve.log_msg( 'The temporary table name: ' || l_temp_table, 5 );
      -- check to make sure the target table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- check to make sure the source table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_source_table );
      -- first rename the target table to temporary table
      o_ev.change_action( 'rename target table' );
      evolve.exec_sql( p_sql => 'alter table ' || l_tab_name || ' rename to ' || l_temp_table, p_auto => 'yes' );
      -- now rename source to target
      o_ev.change_action( 'rename source table' );
      evolve.exec_sql( p_sql       => 'alter table ' || l_src_name || ' rename to ' || UPPER( p_table ),
                           p_auto      => 'yes' );
      -- now rename temporary to source
      o_ev.change_action( 'rename temp table' );
      evolve.exec_sql( p_sql       => 'alter table ' || l_temp_name || ' rename to ' || UPPER( p_source_table ),
                           p_auto      => 'yes'
                         );
      evolve.log_msg( l_src_name || ' and ' || l_tab_name || ' table names interchanged' );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END rename_tables;

   -- procedure to "swap" two primary key names
   -- both tables have to have primary keys enabled
   PROCEDURE rename_primary_keys( p_owner VARCHAR2, p_table VARCHAR2, p_source_owner VARCHAR2, p_source_table VARCHAR2 )
   IS
      l_src_name          VARCHAR2( 61 )                         := UPPER( p_source_owner || '.' || p_source_table );
      l_tab_name          VARCHAR2( 61 )                         := UPPER( p_owner || '.' || p_table );
      l_temp_key          all_constraints.constraint_name%TYPE := 'TD$_PK' || TO_CHAR( SYSTIMESTAMP, 'mmddyyyyHHMISS' );
      l_temp_idx          all_indexes.index_name%TYPE            := l_temp_key;
      l_temp_idx_name     VARCHAR2( 61 )                         := UPPER( p_owner || '.' || l_temp_idx );
      l_source_key        all_constraints.constraint_name%TYPE;
      l_source_idx        all_indexes.index_name%TYPE;
      l_source_idx_own    all_indexes.owner%TYPE;
      l_source_idx_name   VARCHAR2( 61 );
      l_targ_key          all_constraints.constraint_name%TYPE;
      l_targ_idx          all_indexes.index_name%TYPE;
      l_targ_idx_own      all_indexes.owner%TYPE;
      l_targ_idx_name     VARCHAR2( 61 );
      o_ev                evolve_ot                              := evolve_ot( p_module => 'rename_primary_keys' );
   BEGIN
      o_ev.change_action( 'perform object checks' );
      evolve.log_msg( 'The temporary key name: ' || l_temp_key, 5 );
      -- check to make sure the target table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- check to make sure the source table exists
      td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table );
      o_ev.change_action( 'determine key names' );

      -- get the source key
      BEGIN
         SELECT constraint_name, ac.index_name, NVL( ac.index_owner, ac.owner )
           INTO l_source_key, l_source_idx, l_source_idx_own
           FROM all_constraints ac JOIN all_indexes ai
                ON ac.index_name = ai.index_name AND NVL( ac.index_owner, ac.owner ) = ai.owner
          WHERE ac.table_name = UPPER( p_source_table )
            AND ac.owner = UPPER( p_source_owner )
            AND ac.constraint_type = 'P';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_pk', l_src_name );
      END;

      -- get the target key
      BEGIN
         SELECT constraint_name, ac.index_name, NVL( ac.index_owner, ac.owner )
           INTO l_targ_key, l_targ_idx, l_targ_idx_own
           FROM all_constraints ac JOIN all_indexes ai
                ON ac.index_name = ai.index_name AND NVL( ac.index_owner, ac.owner ) = ai.owner
          WHERE ac.table_name = UPPER( p_table ) AND ac.owner = UPPER( p_owner ) AND ac.constraint_type = 'P';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_pk', l_src_name );
      END;

      l_source_idx_name    := UPPER( l_source_idx_own || '.' || l_source_idx );
      l_targ_idx_name      := UPPER( l_targ_idx_own || '.' || l_targ_idx );
      -- first rename the target key to temporary key
      o_ev.change_action( 'rename target key' );
      evolve.exec_sql( p_sql       =>    'alter table '
                                          || l_tab_name
                                          || ' rename constraint '
                                          || l_targ_key
                                          || ' to '
                                          || l_temp_key,
                           p_auto      => 'yes'
                         );
      -- now rename source to target
      o_ev.change_action( 'rename source key' );
      evolve.exec_sql( p_sql       =>    'alter table '
                                          || l_src_name
                                          || ' rename constraint '
                                          || l_source_key
                                          || ' to '
                                          || l_targ_key,
                           p_auto      => 'yes'
                         );
      -- now rename temporary to source
      o_ev.change_action( 'rename temp key' );
      evolve.exec_sql( p_sql       =>    'alter table '
                                          || l_tab_name
                                          || ' rename constraint '
                                          || l_temp_key
                                          || ' to '
                                          || l_source_key,
                           p_auto      => 'yes'
                         );
      -- first rename the target idx to temporary idx
      o_ev.change_action( 'rename target index' );
      evolve.exec_sql( p_sql => 'alter index ' || l_targ_idx_name || ' rename to ' || l_temp_idx, p_auto => 'yes' );
      -- now rename the source index to the target index
      o_ev.change_action( 'rename source index' );
      evolve.exec_sql( p_sql       => 'alter index ' || l_source_idx_name || ' rename to ' || l_targ_idx,
                           p_auto      => 'yes' );
      -- now rename the temporary index to the source index
      o_ev.change_action( 'rename temporary index' );
      evolve.exec_sql( p_sql       => 'alter index ' || l_temp_idx_name || ' rename to ' || l_source_idx,
                           p_auto      => 'yes' );
      evolve.log_msg(    l_source_key
                          || ' and '
                          || l_targ_key
                          || ' constraint names for owner '
                          || UPPER( p_owner )
                          || ' interchanged'
                        );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END rename_primary_keys;

   -- procedure to "swap" two tables using rename
   PROCEDURE replace_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   )
   IS
      l_src_name       VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_source_table );
      l_tab_name       VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_rows           BOOLEAN        := FALSE;
      l_ddl            LONG;
      e_no_stats       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      e_compress       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_compress, -14646 );
      e_fkeys          EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_fkeys, -2266 );
      o_ev             evolve_ot      := evolve_ot( p_module => 'replace_table' );
   BEGIN
      o_ev.change_action( 'perform object checks' );
      -- check to make sure the target table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      -- check to make sure the source table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_source_table );

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
                       p_table             => p_source_table,
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

      -- build the indexes on the source table
      build_indexes( p_owner             => p_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_tablespace        => p_tablespace,
                     p_concurrent        => p_concurrent,
      		     p_queue_module	 => 'replace_table',
      		     p_queue_action	 => 'rename indexes'
                   );
      -- build the constraints on the source table
      build_constraints( p_owner             => p_owner,
                         p_table             => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_table      => p_table,
                         p_basis             => 'all',
                         p_concurrent        => p_concurrent,
      			 p_queue_module	     => 'replace_table',
      			 p_queue_action	     => 'rename constraints'
                       );
      -- grant privileges on the source table
      object_grants( p_owner              => p_owner,
                     p_object             => p_source_table,
                     p_source_owner       => p_owner,
                     p_source_object      => p_table
                   );

      -- drop constraints on the target table
      BEGIN
         drop_constraints( p_owner => p_owner, p_table => p_table, p_basis => 'all' );
      EXCEPTION
         -- if we try to drop the constraints on an IOT, then we know we need to swap them afterwards
         WHEN drop_iot_key
         THEN
            rename_primary_keys( p_owner             => p_owner,
                                 p_table             => p_table,
                                 p_source_owner      => p_owner,
                                 p_source_table      => p_source_table
                               );
      END;

      -- drop indexes on the target table
      drop_indexes( p_owner => p_owner, p_table => p_table );

      -- rename the tables
      BEGIN
         rename_tables( p_owner => p_owner, p_table => p_table, p_source_table => p_source_table );
      END;

      -- rename the indexes
      o_ev.change_action( 'rename indexes' );
      -- this statement will pull previously entered DDL statements off the queue and execute them
      dequeue_ddl( p_action     => evolve.get_action,
		   p_module     => evolve.get_module,
		   p_concurrent => p_concurrent );


      -- rename the constraints
      o_ev.change_action( 'rename constraints' );
      dequeue_ddl( p_action     => evolve.get_action,
		   p_module 	   => evolve.get_module,
		   p_concurrent => p_concurrent);

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
      p_d_num           NUMBER   DEFAULT 0,
      p_p_num           NUMBER   DEFAULT 65535,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL
   )
   IS
      l_tab_name   VARCHAR2( 61 )   := UPPER( p_owner ) || '.' || UPPER( p_table );
      l_src_name   VARCHAR2( 61 )   := UPPER( p_source_owner ) || '.' || UPPER( p_source_object );
      l_msg        VARCHAR2( 2000 );
      l_ddl        VARCHAR2( 2000 );
      l_pidx_cnt   NUMBER;
      l_idx_cnt    NUMBER;
      l_rows       BOOLEAN          DEFAULT FALSE;
      o_ev         evolve_ot        := evolve_ot( p_module => 'unusable_indexes' );
   BEGIN
      CASE
         WHEN p_partname IS NOT NULL AND( p_source_owner IS NOT NULL OR p_source_object IS NOT NULL )
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible', 'P_PARTNAME with either P_SOURCE_OWNER or P_SOURCE_OBJECT' );
         WHEN p_source_owner IS NULL AND p_source_object IS NOT NULL
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible', 'P_SOURCE_OBJECT without P_SOURCE_OWNEW' );
         ELSE
            NULL;
      END CASE;

      -- test the target table
      td_utils.check_table( p_owner => p_owner, p_table => p_table, p_partname => p_partname );

      -- test the source object
      -- but only if it's specified
      -- make sure it's a table or view
      IF p_source_object IS NOT NULL
      THEN
         td_utils.check_object( p_owner            => p_source_owner, p_object => p_source_object,
                                p_object_type      => 'table$|view' );
      END IF;

      o_ev.change_action( 'populate PARTNAME table' );

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
      o_ev.change_action( 'calculate indexes to affect' );

      FOR c_idx IN ( SELECT *
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
                                                  END ) OVER( PARTITION BY 1 ) num_partitions,
                                             CASE idx_ddl_type
                                                WHEN 'I'
                                                   THEN ai_status
                                                ELSE aip_status
                                             END status, include
                                       FROM ( SELECT index_type, owner, ai.index_name, partition_name,
                                                     aip.partition_position, partitioned, aip.status aip_status,
                                                     ai.status ai_status,
                                                     CASE
                                                        WHEN partition_name IS NULL OR partitioned = 'NO'
                                                           THEN 'I'
                                                        ELSE 'P'
                                                     END idx_ddl_type,
                                                     CASE
                                                        WHEN( p_source_object IS NOT NULL OR p_partname IS NOT NULL
                                                            )
                                                        AND ( partitioned = 'YES' )
                                                        AND partition_name IS NULL
                                                           THEN 'N'
                                                        ELSE 'Y'
                                                     END include
                                               FROM td_part_gtt JOIN all_ind_partitions aip USING( partition_name )
                                                    RIGHT JOIN all_indexes ai
                                                    ON ai.index_name = aip.index_name AND ai.owner = aip.index_owner
                                              WHERE ai.table_name = UPPER( p_table )
                                                AND ai.table_owner = UPPER( p_owner ))
                                      WHERE REGEXP_LIKE( index_type, '^' || p_index_type, 'i' )
                                        AND REGEXP_LIKE( partitioned,
                                                         CASE
                                                            WHEN REGEXP_LIKE( 'global', p_part_type, 'i' )
                                                               THEN 'NO'
                                                            WHEN REGEXP_LIKE( 'local', p_part_type, 'i' )
                                                               THEN 'YES'
                                                            ELSE '.'
                                                         END,
                                                         'i'
                                                       )
                                        -- USE an NVL'd regular expression to determine specific indexes to work on
                                        AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
                                        AND NOT REGEXP_LIKE( index_type, 'iot', 'i' )
                                        AND include = 'Y'
                                   ORDER BY idx_ddl_type, partition_position )
                     WHERE status IN( 'VALID', 'USABLE', 'N/A' ))
      LOOP
         o_ev.change_action( 'execute index DDL' );
         l_rows        := TRUE;
         evolve.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes' );
         l_pidx_cnt    := c_idx.num_partitions;
         l_idx_cnt     := c_idx.num_indexes;
      END LOOP;

      IF l_rows
      THEN
         IF l_idx_cnt > 0 OR l_pidx_cnt > 0
         THEN
            evolve.log_msg(    l_idx_cnt
                                || ' index'
                                || CASE l_idx_cnt
                                      WHEN 1
                                         THEN NULL
                                      ELSE 'es'
                                   END
                                || ' and '
                                || l_pidx_cnt
                                || ' local index partition'
                                || CASE l_pidx_cnt
                                      WHEN 1
                                         THEN NULL
                                      ELSE 's'
                                   END
                                || ' affected on table '
                                || l_tab_name
                              );
         END IF;
      ELSE
         evolve.log_msg( 'No matching usable indexes found on ' || l_tab_name );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END unusable_indexes;

   -- rebuilds all unusable index segments on a particular table
   PROCEDURE usable_indexes( p_owner VARCHAR2, p_table VARCHAR2, p_concurrent VARCHAR2 DEFAULT 'no' )
   IS
      l_ddl             VARCHAR2( 2000 );
      l_rows            BOOLEAN          := FALSE;                                            -- to catch empty cursors
      l_cnt             NUMBER           := 0;
      l_tab_name        VARCHAR2( 61 )   := UPPER( p_owner || '.' || p_table );
      l_concurrent_id   VARCHAR2( 100 );
      o_ev              evolve_ot        := evolve_ot( p_module => 'usable_indexes', p_action => 'Rebuild indexes' );
   BEGIN
      td_utils.check_table( p_owner => p_owner, p_table => p_table );

      IF td_utils.is_part_table( p_owner, p_table )
      THEN
         -- need to get a unique "job header" number in case we are running concurrently
         o_ev.change_action( 'get concurrent id' );

         IF td_core.is_true( p_concurrent )
         THEN
            l_concurrent_id    := evolve.get_concurrent_id;
         END IF;

         -- rebuild local indexes first
         o_ev.change_action( 'rebuild local indexes' );

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
                          WHERE table_name = UPPER( p_table ) AND table_owner = UPPER( p_owner )
                       ORDER BY table_name, partition_position )
         LOOP
            evolve.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes', p_concurrent_id => l_concurrent_id );
            l_cnt    := l_cnt + 1;
         END LOOP;

         evolve.log_msg(    'Rebuild processes for any unusable indexes on '
                             || l_cnt
                             || ' partition'
                             || CASE
                                   WHEN l_cnt = 1
                                      THEN NULL
                                   ELSE 's'
                                END
                             || ' of table '
                             || l_tab_name
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'executed'
                                END
                           );
      END IF;

      IF td_core.is_true( p_concurrent )
      THEN
         -- now simply waiting for all the concurrent processes to complete
         o_ev.change_action( 'wait on concurrent processes' );
         evolve.coordinate_sql( p_concurrent_id => l_concurrent_id, p_raise_err => 'no' );
      END IF;

      -- reset variables
      l_cnt     := 0;
      l_rows    := FALSE;
      -- get another concurrent_id
      o_ev.change_action( 'get concurrent id' );

      IF td_core.is_true( p_concurrent )
      THEN
         l_concurrent_id    := evolve.get_concurrent_id;
      END IF;

      -- now see if any global are still unusable
      o_ev.change_action( 'rebuild global indexes' );

      FOR c_gidx IN ( SELECT  table_name,
                              'alter index ' || owner || '.' || index_name || ' rebuild parallel nologging' DDL
                         FROM all_indexes
                        WHERE table_name = UPPER( p_table )
                          AND table_owner = UPPER( p_owner )
                          AND status = 'UNUSABLE'
                          AND partitioned = 'NO'
                     ORDER BY table_name )
      LOOP
         l_rows    := TRUE;
         evolve.exec_sql( p_sql => c_gidx.DDL, p_auto => 'yes', p_concurrent_id => l_concurrent_id );
         l_cnt     := l_cnt + 1;
      END LOOP;

      IF l_rows
      THEN
         IF td_core.is_true( p_concurrent )
         THEN
            -- now simply waiting for all the concurrent processes to complete
            o_ev.change_action( 'wait on concurrent processes' );
            evolve.coordinate_sql( p_concurrent_id => l_concurrent_id, p_raise_err => 'no' );
         END IF;

         evolve.log_msg(    l_cnt
                             || CASE
                                   WHEN td_utils.is_part_table( p_owner, p_table )
                                      THEN ' global'
                                   ELSE NULL
                                END
                             || ' index rebuild process'
                             || CASE l_cnt
                                   WHEN 1
                                      THEN NULL
                                   ELSE 'es'
                                END
                             || ' for table '
                             || l_tab_name
                             || ' '
                             || CASE
                                   WHEN td_core.is_true( p_concurrent )
                                      THEN 'submitted to the Oracle scheduler'
                                   ELSE 'executed'
                                END
                           );
      ELSE
         evolve.log_msg(    'No matching unusable '
                             || CASE
                                   WHEN td_utils.is_part_table( p_owner, p_table )
                                      THEN 'global '
                                   ELSE NULL
                                END
                             || 'indexes found'
                           );
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
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
                              := 'TD$' || SYS_CONTEXT( 'USERENV', 'SESSIONID' )
                                 || TO_CHAR( SYSDATE, 'yyyymmdd_hhmiss' );
      e_no_stats    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      l_rows        BOOLEAN        := FALSE;                                                  -- to catch empty cursors
      o_ev          evolve_ot      := evolve_ot( p_module => 'update_stats' );
   BEGIN
      -- check all the parameter requirements
      CASE
         WHEN    p_source_owner IS NOT NULL AND p_source_table IS NULL
              OR ( p_source_owner IS NULL AND p_source_table IS NOT NULL )
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible', 'P_SOURCE_OWNER and P_SOURCE_OBJECT are mutually inclusive' );
         WHEN p_source_partname IS NOT NULL AND( p_source_owner IS NULL OR p_source_table IS NULL )
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible',
                                  'P_SOURCE_PARTNAME requires P_SOURCE_OWNER and P_SOURCE_OBJECT'
                                );
         WHEN p_partname IS NOT NULL AND( p_owner IS NULL OR p_table IS NULL )
         THEN
            o_ev.clear_app_info;
            evolve.raise_err( 'parms_not_compatible', 'P_PARTNAME requires P_OWNER and P_OBJECT' );
         ELSE
            NULL;
      END CASE;

      -- verify the structure of the target table
      -- this is only applicable if a table is having stats gathered, instead of a schema
      IF p_table IS NOT NULL
      THEN
         td_utils.check_table( p_owner => p_owner, p_table => p_table, p_partname => p_partname );
      END IF;

      -- verify the structure of the source table (if specified)
      IF ( p_source_owner IS NOT NULL OR p_source_table IS NOT NULL )
      THEN
         td_utils.check_table( p_owner => p_source_owner, p_table => p_source_table, p_partname => p_source_partname );
      END IF;

      -- check to see if we are in debug mode
      IF NOT evolve.is_debugmode
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
	       o_ev.change_action( 'gathering schema stats' );
               DBMS_STATS.gather_schema_stats( ownname               => p_owner,
                                               estimate_percent      => NVL( p_percent, DBMS_STATS.auto_sample_size ),
                                               method_opt            => p_method,
                                               DEGREE                => NVL( p_degree, DBMS_STATS.auto_degree ),
                                               granularity           => p_granularity,
                                               CASCADE               => NVL( td_core.is_true( p_cascade, TRUE ),
                                                                             DBMS_STATS.auto_cascade
                                                                           ),
                                               options               => p_options
                                             );
            -- if the table name is not null, then we are only collecting stats on a particular table
            -- will call GATHER_TABLE_STATS as opposed to GATHER_SCHEMA_STATS
            ELSE
	       o_ev.change_action( 'gathering table stats' );
               DBMS_STATS.gather_table_stats( ownname               => p_owner,
                                              tabname               => p_table,
                                              partname              => p_partname,
                                              estimate_percent      => NVL( p_percent, DBMS_STATS.auto_sample_size ),
                                              method_opt            => p_method,
                                              DEGREE                => NVL( p_degree, DBMS_STATS.auto_degree ),
                                              granularity           => p_granularity,
                                              CASCADE               => NVL( td_core.is_true( p_cascade, TRUE ),
                                                                            DBMS_STATS.auto_cascade
                                                                          )
                                            );
            END IF;
         -- if the source owner isn't null, then we know we are transferring statistics
         -- we will use GET_TABLE_STATS and PUT_TABLE_STATS
         ELSE
            o_ev.change_action( 'export stats' );
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
               WHEN     td_utils.is_part_table( p_owner => p_source_owner, p_table => p_source_table )
                    -- and the target table is not partitioned
                    AND NOT td_utils.is_part_table( p_owner => p_owner, p_table => p_table )
                  -- then delete the partition level information from the stats table
               THEN
                     DELETE FROM opt_stats
                           WHERE statid = l_statid AND( c2 IS NOT NULL OR c3 IS NOT NULL );
                  ELSE
                     NULL;
               END CASE;

               -- now import the statistics
               o_ev.change_action( 'import stats' );
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

      evolve.log_msg(    'Statistics '
                          || CASE
                                WHEN p_source_table IS NULL
                                   THEN 'gathered on '
                                ELSE    'from '
                                     || CASE
                                           WHEN p_source_partname IS NULL
                                              THEN NULL
                                           ELSE 'partition ' || UPPER( p_source_partname ) || ' of '
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
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END update_stats;
END td_dbutils;
/

SHOW errors