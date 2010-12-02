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
      p_partid          VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 3,
      p_p_num           NUMBER DEFAULT 1048576
   )
   AS
      o_ev              evolve_ot                                    := evolve_ot( p_module => 'populate_partname' );
      l_dsql            LONG;
      l_num_msg         VARCHAR2( 100 )                              := 'Number of records inserted into TD_PART_GTT table';
      l_src_part_col    all_part_key_columns.column_name%TYPE;
      l_src_spart_col   all_subpart_key_columns.column_name%TYPE;
      l_results         NUMBER;
      l_part_position   all_tab_partitions.partition_position%TYPE;
      l_high_value      all_tab_partitions.high_value%TYPE;
      l_num_rows        NUMBER;
      l_part_type       VARCHAR2(10);
      l_part_name       all_tab_partitions.partition_name%TYPE;

   BEGIN
      td_utils.check_table( p_owner => p_owner, p_table => p_table, p_partname => p_partname, p_partitioned => 'yes' );
      
      -- find out if the table is partitioned or subpartitioned
      l_part_type := td_utils.get_tab_part_type( p_owner, p_table );
      evolve.log_variable('l_part_type',l_part_type);
            
      -- get the default partname, which is the max partition
      IF p_partname IS NOT NULL
      THEN
         BEGIN

            IF l_part_type = 'part'
            THEN
               
               SELECT partition_position, high_value
                 INTO l_part_position, l_high_value
                 FROM all_tab_partitions
                WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table )
                  AND partition_name = UPPER( p_partname );
               
            ELSIF l_part_type = 'subpart'
            THEN
               
               SELECT subpartition_position, high_value
                 INTO l_part_position, l_high_value
                 FROM all_tab_subpartitions
                WHERE table_owner = UPPER( p_owner ) AND table_name = UPPER( p_table )
                  AND subpartition_name = UPPER( p_partname );
               
               -- find out the partition_name in case it's subpartitioned
               l_part_name := td_utils.get_part_for_subpart( p_owner => p_owner, p_segment => p_table, p_subpart => p_partname, p_segment_type => 'table' );
               evolve.log_variable('l_part_name',l_part_name);
               
            END IF;
         EXCEPTION
            WHEN no_data_found
            THEN
               evolve.raise_err( 'no_part', p_partname );
         END;
         
         evolve.log_variable( 'l_part_position', l_part_position );
         evolve.log_variable( 'l_high_value', l_high_value );
         
         -- write records to the global temporary table, which will later be used in cursors for other procedures

         -- if P_PARTNAME is null, then we want the max partition
         -- go ahead and write that single record
         o_ev.change_action( 'static insert' );

         INSERT INTO td_part_gtt
                ( table_owner, table_name, partition_name, partition_position, partid
                     )
                VALUES ( UPPER( p_owner ), UPPER( p_table ), UPPER( p_partname ), l_part_position, p_partid
                     );
         evolve.log_msg( SQL%ROWCOUNT||' rows inserted into TD_PART_GTT', 4 );

      ELSE

         SELECT nvl(p_source_column, pk.column_name),
                sk.column_name
           INTO l_src_part_col,
                l_src_spart_col
           FROM all_part_key_columns pk
           left JOIN all_subpart_key_columns sk
                USING (owner,name)
          WHERE NAME = UPPER( p_table ) AND owner = UPPER( p_owner );

         evolve.log_variable('l_src_part_col',l_src_part_col);
         evolve.log_variable('l_src_spart_col',l_src_spart_col);
               

         o_ev.change_action( 'dynamic insert' );
         
         l_dsql := 
         'insert into td_part_gtt ( table_owner, table_name, partition_name, partition_position, partid ) '
         || ' SELECT owner, object_name, subobject_name, object_id, '''||p_partid
         || '''  FROM all_objects'
         || ' WHERE owner = '''
         || UPPER( p_owner )
         || ''' AND object_name = '''
         || UPPER( p_table )
         || ''' AND object_id IN '
         || ' (SELECT DISTINCT tbl$or$idx$part$num("'
         || UPPER( p_owner )
         || '"."'
         || UPPER( p_table )
         || '", 0, '
         || p_d_num
         || ', '
         || p_p_num
         || ', "'
         || UPPER( l_src_part_col )
         || '"'
         || CASE l_part_type WHEN 'subpart' THEN ', "'||upper(l_src_spart_col)||'"' ELSE NULL END
         || ')	 FROM '
         || UPPER( p_source_owner )
         || '.'
         || UPPER( p_source_object )
         || ') '
         || 'ORDER By object_id';
         
         evolve.log_msg( 'Dynamic tbl$or$idx$part$num statement: '||l_dsql, 4 );
         
         EXECUTE IMMEDIATE l_dsql;

         evolve.log_msg( SQL%ROWCOUNT||' rows inserted into TD_PART_GTT', 4 );

      END IF;

      -- get count of records affected
      SELECT COUNT( * )
        INTO l_num_rows
        FROM td_part_gtt;

      evolve.log_msg( 'Number of records currently in TD_PART_GTT:' || l_num_rows, 5 );
      COMMIT;
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

      -- register variables
      evolve.log_variable( 'p_stmt',    p_stmt );
      evolve.log_variable( 'p_msg',     p_msg );
      evolve.log_variable( 'p_module',  p_module );
      evolve.log_variable( 'p_action',  p_action );     
      evolve.log_variable( 'p_order',   p_order );     

      INSERT INTO ddl_queue
             ( stmt_ddl, stmt_msg, module, action, stmt_order
             )
             VALUES ( p_stmt, p_msg, p_module, p_action, p_order
                    );
      COMMIT;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END enqueue_ddl;
   
   FUNCTION dequeue_ddl( 
      p_module        VARCHAR2,
      p_action	      VARCHAR2,
      p_concurrent    VARCHAR2 DEFAULT 'no',
      p_raise_err     VARCHAR2 DEFAULT 'yes'
   )
      RETURN NUMBER 
   IS
      l_stmt_cnt         NUMBER    := 0;
      l_stmtcurrent_id   VARCHAR2( 100 ) := NULL;
      l_rows            BOOLEAN   := FALSE;
      -- purposefully not initiating an EVOLVE_OT object
      -- this procedure needs to be transparent for a reason
      o_ev              evolve_ot := evolve_ot( p_module => 'dequeue_ddl' );
   BEGIN
      
      -- register variables
      evolve.log_variable( 'p_concurrent',p_concurrent );
      evolve.log_variable( 'p_raise_err',p_raise_err );
      evolve.log_variable( 'p_module',p_module );
      evolve.log_variable( 'p_action',p_action );

      -- need to get a unique "job header" number in case we are running concurrently
      IF td_core.is_true( p_concurrent )
      THEN
	 o_ev.change_action( 'get concurrent id' );
         l_stmtcurrent_id    := evolve.get_concurrent_id;
      END IF;

      o_ev.change_action( 'looping through DDL' );
      -- looping through records in the DDL_QUEUE table
      -- finding statements queueud there previously
      evolve.log_msg( 'Executing DDL statements previously queued', 3 );
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
	    
            IF NOT evolve.is_debugmode
            THEN

	       -- delete the row from the queue once it's executed
	       DELETE FROM ddl_queue
	        WHERE ROWID = c_stmts.rowid;
               
            END IF;

            l_stmt_cnt    := l_stmt_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No queued DDL statements applicable for this module and action', 3 );
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
                                END, 3
                           );
      END IF;
      
      o_ev.clear_app_info;
      RETURN l_stmt_cnt;
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
      p_constraints    VARCHAR2 DEFAULT 'no',
      p_indexes	       VARCHAR2 DEFAULT 'no',
      p_partitioning   VARCHAR2 DEFAULT 'keep',
      p_grants         VARCHAR2 DEFAULT 'no',
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
      l_part_col       all_part_key_columns.column_name%TYPE;
      l_col1           all_tab_columns.column_name%TYPE;
      l_rows           BOOLEAN                       := FALSE;
      e_dup_idx_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_idx_name, -955 );
      e_dup_col_list   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_col_list, -1408 );
      o_ev             evolve_ot                     := evolve_ot( p_module => 'build_table' );
   BEGIN
      -- confirm that the source table exists
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
                                            CASE 

                                            -- we want to keep partitioning
                                            WHEN REGEXP_LIKE( 'keep', p_partitioning, 'i' )
                                            -- don't do anything
                                            -- keep the partitioning information exactly how it is
                                            THEN NULL

                                            -- in any other situations, we don't want partitioning
                                            -- this is "remove" or "single"
                                            ELSE '(\(\s*partition.+\))\s*|(partition by).+\)\s*'

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
                               WHEN p_tablespace = default_tablespace
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
                

      o_ev.change_action( 'single-part check' );

      -- if p_partitioning is 'single', then we have a few steps
      CASE
        WHEN REGEXP_LIKE( 'single', p_partitioning, 'i' )
        THEN
           -- get the first column attribute to use as the partition key
           -- this is a single partition table, so it doesn't matter what we use
           SELECT column_name
             INTO l_col1
             FROM all_tab_columns
            WHERE table_name = upper(p_source_table)
              AND owner      = upper(p_source_owner)
              AND column_id = 1;

           l_table_ddl := l_table_ddl 
           || ' partition by range ('||l_col1||')'
           || ' ( partition pmax values less than (maxvalue))';
        WHEN REGEXP_LIKE( 'keep', p_partitioning, 'i' )
        THEN
           NULL;
        WHEN REGEXP_LIKE( 'remove', p_partitioning, 'i' )
        THEN
           NULL;
        ELSE
            evolve.raise_err( 'unrecognized_parm', p_partitioning );
      END CASE;

      
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
	 COMMIT;
      END IF;

      -- we want to gather statistics
      -- we gather statistics first before the indexes are built
      -- the indexes will collect there own statistics when they are built
      -- that is why we don't cascade
      CASE
         WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
         THEN
            gather_stats( p_owner => p_owner, p_segment => p_table, p_segment_type=>'table' );
         -- we want to transfer the statistics from the current segment into the new segment
         -- this is preferable if automatic stats are handling stats collection
         -- and you want the load time not to suffer from statistics gathering
      WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
         THEN
            transfer_stats( p_owner             => p_owner,
                            p_segment           => p_table,
                            p_source_owner      => p_source_owner,
                            p_source_segment    => p_source_table,
                            p_segment_type      => 'table'
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


      -- do we want indexes as well
      IF td_core.is_true( p_indexes )
      THEN
         o_ev.change_action( 'build indexes' );
         build_indexes( p_source_owner       => p_source_owner,
                      	p_source_table       => p_source_table,
                       	p_owner              => p_owner,
                       	p_table              => p_table,
			p_tablespace 	     => p_tablespace
                     );
      END IF;
      
      
      -- do we want constraints as well
      IF td_core.is_true( p_constraints )
      THEN
         o_ev.change_action( 'build indexes' );
         build_constraints( p_source_owner       => p_source_owner,
                      	    p_source_table       => p_source_table,
                       	    p_owner              => p_owner,
                       	    p_table              => p_table
			  );
      END IF;

      -- do we want grants as well
      IF td_core.is_true( p_grants )
      THEN
         o_ev.change_action( 'grant privilges' );
         object_grants( p_source_owner       => p_source_owner,
                        p_source_object      => p_source_table,
                       	p_owner              => p_owner,
                       	p_object             => p_table
		      );
      END IF;

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
      l_src_name        VARCHAR2( 61 )                               := UPPER( p_source_owner || '.' || p_source_table );
      l_part_type       VARCHAR2( 6 );
      l_src_part        BOOLEAN;
      l_targ_part       BOOLEAN;
      l_src_part_flg    all_tables.partitioned%TYPE;
      l_targ_part_flg   all_tables.partitioned%TYPE;
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
      
      -- register variables
      evolve.log_variable( 'p_concurrent',p_concurrent );
      evolve.log_variable( 'p_partname',p_partname );
      evolve.log_variable( 'p_queue_module',p_queue_module );
      evolve.log_variable( 'p_queue_action',p_queue_action );

      -- find out which tables are partitioned
      l_src_part       := td_utils.is_part_table( p_source_owner, p_source_table);
      l_src_part_flg   := CASE WHEN l_src_part THEN 'yes' ELSE 'no' END;
      l_targ_part      := td_utils.is_part_table( p_owner, p_table );
      l_targ_part_flg  := CASE WHEN l_targ_part THEN 'yes' ELSE 'no' END;


      o_ev.change_action( 'check objects' );
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

      -- if P_PARTNAME is specified, then I need the partition position
      IF p_partname IS NOT NULL
      THEN
         SELECT partition_position
           INTO l_part_position
           FROM ( SELECT table_name,
                         CASE WHEN subpartition_name IS NULL THEN partition_position ELSE subpartition_position END partition_position,
                         table_owner,
                         CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                         CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                    FROM  all_tab_partitions ip
                    left JOIN all_tab_subpartitions isp
                         USING (table_owner, table_name, partition_name ))
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
                                                  WHEN l_targ_part_flg = 'no' AND p_tablespace IS NULL
                                                       AND p_partname IS NULL
                                                        -- remove all partitioning and the local keyword
                                                  THEN '\s*(\(\s*partition.+\))|local\s*'
                                                     -- target is not partitioned but p_TABLESPACE or p_PARTNAME is provided
                                                  WHEN l_targ_part_flg = 'no'
                                                  AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                        -- strip out partitioned info and local keyword and tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|local|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned and p_TABLESPACE or p_PARTNAME is provided
                                                  WHEN l_targ_part_flg = 'ys'
                                                  AND ( p_tablespace IS NOT NULL OR p_partname IS NOT NULL )
                                                        -- strip out partitioned info keeping local keyword and remove tablespace clause
                                                  THEN '\s*(\(\s*partition.+\))|(tablespace)\s*\S+\s*'
                                                     -- target is partitioned
                                                     -- p_TABLESPACE is null
                                                     -- p_PARTNAME is null
                                                  WHEN l_targ_part_flg = 'yes' AND p_tablespace IS NULL
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
                                                -- if constant default_tablespace is passed, then use the users default tablespace
                                                -- a non-null value for p_tablespace already stripped all tablespace information above
                                                -- now just need to not put in the 'TABLESPACE' information here
                                             WHEN LOWER( p_tablespace ) = default_tablespace
                                                   THEN NULL
                                                -- if p_TABLESPACE is provided, then previous tablespace information was stripped (above)
                                                -- now we can just tack the new tablespace information on the end
                                             WHEN p_tablespace IS NOT NULL
                                                   THEN ' TABLESPACE ' || UPPER( p_tablespace )
                                                WHEN p_partname IS NOT NULL
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
                                                                     AND partition_position = l_part_position )
                                                              )
                                                ELSE NULL
                                             END 
                                          || CASE WHEN td_core.get_yn_ind( l_targ_part_flg ) = 'yes' AND td_core.get_yn_ind( l_src_part_flg ) = 'no' THEN ' LOCAL' ELSE NULL END
                                          index_ddl,
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
                                3
                              );
            l_idx_cnt    := l_idx_cnt + 1;

            o_ev.change_action( 'enqueue idx rename DDL' );
            
            -- queue up alternative DDL statements for later use
            -- in this case, queue up index rename statements
            -- these statements are used by module 'replace_table' and action 'rename indexes'
            IF p_queue_module = 'replace_table' AND p_queue_action = 'build indexes'
            THEN
	       enqueue_ddl( p_stmt     => c_indexes.rename_ddl,
			    p_msg      => c_indexes.rename_msg,
			    p_module   => p_queue_module,
			    p_action   => 'rename indexes' );
            END IF;

            o_ev.change_action( 'enqueue idx rename DDL' );

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
         evolve.log_msg( 'No matching indexes found on ' || l_src_name, 1 );
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

       evolve.log_variable('l_targ_part',l_targ_part);
       evolve.log_variable('l_iot_type', l_iot_type);

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

       evolve.log_variable('l_part_position',l_part_position);

      IF td_core.is_true( p_concurrent )
      THEN

         -- need to get a unique "job header" number in case we are running concurrently
         o_ev.change_action( 'get concurrent id' );

         l_concurrent_id    := evolve.get_concurrent_id;

      END IF;

       evolve.log_variable('l_concurrent_id',l_concurrent_id);

      o_ev.change_action( 'main cursor' );

      FOR c_constraints IN
         (
           -- this case statement uses GENERIC_CON column to determine the final index name
           -- GENERIC_CON is a case statement that is generated below
           -- IF we are using a generic name, then perform the replace
          SELECT constraint_owner, CASE generic_con
                 WHEN 'Y'
                 THEN con_rename_adj
                 ELSE con_rename
                 END constraint_name, 
                 source_owner, 
                 source_table, 
                 source_constraint, 
                 constraint_type, 
                 index_owner,
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
                 
                 -- this column was added for the EXCHANGE_PARTITION procedure
                 -- so we can drop only the constraints we added
                      ' alter table '
                   || source_owner
                   || '.'
                   || source_table
                   || ' drop constraint '
                   || CASE generic_con
                         WHEN 'Y'
                            THEN con_rename_adj
                         ELSE con_rename
                      END drop_ddl,
                      
                 -- this column was added for the EXCHANGE_PARTITION procedure
                 -- so we can drop only the constraints we added
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
                   || ' dropped' drop_msg,

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
                                          -- also, IF constant default_tablespace is passed, then use the users default tablespace
                                       WHEN ac.index_name IS NULL OR LOWER( p_tablespace ) = default_tablespace
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

               o_ev.change_action( 'exec constraint DDL' );

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

               -- only insert for rename if the constraint is a named constraint
               -- queue up alternative DDL statements for later use
               -- in this case, queue up constraint rename statements
               -- these statements are used by module 'replace_table' and action 'rename constraints'


               IF c_constraints.named_constraint = 'Y' 
                  AND p_queue_module = 'replace_table'
                  AND p_queue_action = 'build constraints'
               THEN

                  o_ev.change_action( 'enqueue build idx DDL' );

		  enqueue_ddl( p_stmt	     => c_constraints.rename_ddl,
			       p_msg  	     => c_constraints.rename_msg,
			       p_module	     => p_queue_module,
			       p_action	     => 'rename constraints' );
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
         evolve.log_msg( 'No matching constraints found on ' || l_src_name, 1 );
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

      FOR c_constraints IN ( SELECT 
                                    CASE maint_type
                                    WHEN 'validate' THEN validate_ddl
                                    WHEN 'disable' THEN disable_ddl
                                    WHEN 'disable validate' THEN disable_ddl
                                    WHEN 'enable' THEN enable_ddl
                                    ELSE NULL END ddl,
                                    CASE maint_type
                                    WHEN 'validate' THEN validate_msg
                                    WHEN 'disable' THEN disable_msg
                                    WHEN 'disable validate' THEN disable_msg
                                    WHEN 'enable' THEN enable_msg
                                    ELSE NULL END msg,
                                    ordering, basis_source, table_owner, table_name, constraint_name,
                                    disable_ddl, disable_msg, enable_ddl, enable_msg, validate_ddl, validate_msg, 
                                    basis_include, maint_type
                               FROM ( SELECT 
                                             -- need to specify the kind of constraint maintenance that is to be performed
                                             CASE
                                             WHEN lower( p_maint_type ) = 'validate' AND status = 'DISABLED' AND validated = 'NOT VALIDATED'
                                             THEN 'validate'
                                             WHEN lower( p_maint_type ) = 'disable' AND status = 'DISABLED' AND validated = 'VALIDATED'
                                             THEN 'disable validate'
                                             WHEN lower( p_maint_type ) = 'disable' AND status = 'ENABLED'
                                             THEN 'disable'         
                                             WHEN lower( p_maint_type ) = 'enable' AND status = 'DISABLED'
                                             THEN 'enable'
                                             ELSE 'none'
                                             END maint_type,
                                             ordering, basis_source, table_owner, table_name, constraint_name,
                                             disable_ddl, disable_msg, enable_ddl, enable_msg, validate_ddl, validate_msg, 
                                             basis_include
                                        FROM ( SELECT
                                                      -- need this to get the order by clause right
                                                      -- WHEN we are disabling, we need references to go first
                                                      -- WHEN we are enabling, we need referenced (primary keys) to go first
                                                      CASE lower( p_maint_type )
                                                      WHEN 'enable'
                                                      THEN 1
                                                      ELSE 2
                                                      END ordering, 'table' basis_source, owner table_owner, table_name,
                                                      constraint_name, status, validated,
                                                      'alter table '
                                                      || l_tab_name
                                                      || ' modify constraint '
                                                      || constraint_name
                                                      || ' validate' validate_ddl,
                                                      'Constraint '
                                                      || constraint_name
                                                      || ' validated on '
                                                      || l_tab_name validate_msg,
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
                                                      END basis_include
                                                 FROM all_constraints
                                                WHERE table_name = upper( p_table )
                                                  AND owner = upper( p_owner )
                                                  AND REGEXP_LIKE( constraint_name, nvl( p_constraint_regexp, '.' ), 'i' )
                                                  AND REGEXP_LIKE( constraint_type, nvl( p_constraint_type, '.' ), 'i' )
                                                UNION
                                               SELECT
                                                      -- need this to get the order by clause right
                                                      -- WHEN we are disabling, we need references to go first
                                                      -- WHEN we are enabling, we need referenced (primary keys) to go first
                                                      CASE lower( p_maint_type )
                                                      WHEN 'enable'
                                                      THEN 2
                                                      ELSE 1
                                                      END ordering, 'reference' basis_source, owner table_owner, table_name,
                                                      constraint_name, status, validated,
                                                      'alter table '
                                                      || owner
                                                      || '.'
                                                      || table_name
                                                      || ' modify constraint '
                                                      || constraint_name
                                                      || ' validate' validate_ddl,
                                                      'Constraint '
                                                      || constraint_name
                                                      || ' validated on '
                                                      || owner
                                                      || '.'
                                                      || table_name validate_msg,
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
                                                      END basis_include
                                                 FROM all_constraints
                                                WHERE constraint_type = 'R'
                                                  AND REGEXP_LIKE( constraint_name, nvl( p_constraint_regexp, '.' ), 'i' )
                                                  AND r_constraint_name IN (
                                                                             SELECT constraint_name
                                                                               FROM all_constraints
                                                                              WHERE table_name = upper( p_table )
                                                                                AND owner = upper( p_owner )
                                                                                AND constraint_type = 'P' )
                                                  AND r_owner IN (
                                                                   SELECT owner
                                                                     FROM all_constraints
                                                                    WHERE table_name = upper( p_table )
                                                                      AND owner = upper( p_owner )
                                                                      AND constraint_type = 'P' )
                                             )
                                    )
                              WHERE basis_include = 'Y'
                                AND maint_type <> 'none'
                              ORDER BY ordering )
      LOOP
         -- catch empty cursor sets
         l_rows    := TRUE;

         BEGIN
            evolve.exec_sql( p_sql                => c_constraints.ddl,
                             p_auto               => 'yes',
                             p_concurrent_id      => l_concurrent_id
                           );

            -- queue up alternative DDL statements for later use
            -- first, queue up any ENABLE statements that need to be executed
            IF c_constraints.maint_type = 'disable'
            THEN
               o_ev.change_action( 'enqueue enable con DDL' );
	       enqueue_ddl( p_stmt	  => c_constraints.enable_ddl,
			    p_msg  	  => c_constraints.enable_msg,
			    p_module	  => p_queue_module,
			    p_action	  => p_queue_action );

            END IF;

            -- now queue up VALIDATE statements
            IF c_constraints.maint_type = 'validate'
            THEN
               o_ev.change_action( 'enqueue enable con DDL' );
	       enqueue_ddl( p_stmt	  => c_constraints.validate_ddl,
			    p_msg  	  => c_constraints.validate_msg,
			    p_module	  => p_queue_module,
			    p_action	  => p_queue_action );

            END IF;
            
            evolve.log_msg( c_constraints.msg, 3 );
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
                                   WHEN REGEXP_LIKE( 'validate', p_maint_type, 'i' )
                                      THEN 'disabled'
                                END
                             || ' constraints found.', 2
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
            evolve.log_msg( 'Index ' || c_indexes.full_index_name || ' dropped', 3 );
         EXCEPTION
            WHEN e_pk_idx
            THEN
               NULL;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve.log_msg( 'No matching indexes to drop found on ' || l_tab_name, 1 );
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
                                         AND r_owner IN (
                                                          SELECT owner
                                                            FROM all_constraints
                                                           WHERE table_name = UPPER( p_table )
                                                             AND owner = UPPER( p_owner )
                                                             AND constraint_type = 'P' )
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
            evolve.log_msg( 'Constraint ' || c_constraints.constraint_name || ' dropped', 3 );
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
         evolve.log_msg( 'No matching constraints to drop found on ' || l_tab_name, 1 );
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
	 -- when there are no object grants found
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
      
      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF td_core.is_true( p_direct ) AND p_log_table IS NOT NULL
      THEN
         
         o_ev.change_action( 'issue log_errors warning' );

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
      
      IF td_core.is_true( p_direct )
      THEN
            
         evolve.exec_sql( 'commit' );

      END IF;

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
         evolve.log_results_msg( p_count      => SQL%ROWCOUNT,
                                 p_owner      => p_owner,
                                 p_object     => p_table,
                                 p_category   => 'insert',
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
         IF td_core.is_true( p_direct )
         THEN
            
            evolve.exec_sql( 'commit' );
         END IF;

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
         evolve.log_results_msg( p_count      => SQL%ROWCOUNT,
                                 p_owner      => p_owner,
                                 p_object     => p_table,
                                 p_category   => 'merge',
                                 p_msg        => 'Number of records merged into ' || l_trg_name );
         
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
      p_owner             VARCHAR2,
      p_table             VARCHAR2,
      p_source_owner      VARCHAR2,
      p_source_table      VARCHAR2,
      p_partname          VARCHAR2 DEFAULT NULL,
      p_index_space       VARCHAR2 DEFAULT NULL,
      p_idx_concurrency   VARCHAR2 DEFAULT 'no',
      p_con_concurrency   VARCHAR2 DEFAULT 'no',
      p_drop_deps         VARCHAR2 DEFAULT 'yes',
      p_statistics        VARCHAR2 DEFAULT 'transfer',
      p_statpercent       NUMBER DEFAULT NULL,
      p_statdegree        NUMBER DEFAULT NULL,
      p_statmethod        VARCHAR2 DEFAULT NULL
   )
   IS
      
      -- variables to hold table and owner names based on PART or NONPART
      l_part_table     all_tables.table_name%TYPE;
      l_nonpart_table  all_tables.table_name%TYPE;
      l_part_owner     all_tables.owner%TYPE;
      l_nonpart_owner  all_tables.owner%TYPE;

      -- variable to hold full qualified table names based on SOURCE or TARGET
      l_src_full       VARCHAR2( 61 )                   := UPPER( p_source_owner || '.' || p_source_table );
      l_tab_full       VARCHAR2( 61 )                   := UPPER( p_owner || '.' || p_table );

      -- variables to hold fully qualified table names based on PART or NONPART
      l_part_full      VARCHAR2( 61 );
      l_nonpart_full   VARCHAR2( 61 );
      
      -- misc variables
      l_partname       all_tab_partitions.partition_name%TYPE;
      l_ddl            LONG;
      l_num_cons       NUMBER;
      l_part_type      VARCHAR2(10);
      
      -- Booleans to handle the EXIT LOOP for issuing the EXCHANGE command
      l_build_cons     BOOLEAN                          := FALSE;
      l_compress       BOOLEAN                          := FALSE;
      l_constraints    BOOLEAN                          := FALSE;
      l_retry_ddl      BOOLEAN                          := FALSE;
      l_src_part       BOOLEAN;
      l_trg_part       BOOLEAN;
      
      -- escape hatch functionality for the EXIT WHEN loop on the EXCHANGE
      l_exit_cnt      NUMBER                            := 0;
            
      -- catch empty cursors
      l_rows           BOOLEAN                          := FALSE;
      
      -- exceptions
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
      
      -- Evolve instrumentation
      o_ev             evolve_ot                        := evolve_ot( p_module => 'exchange_partition' );
   BEGIN
      
      -- log variable values      
      evolve.log_variable( 'p_source_owner',     p_source_owner );      
      evolve.log_variable( 'p_source_table',     p_source_table );
      evolve.log_variable( 'l_src_full',         l_src_full );
      evolve.log_variable( 'l_tab_full',         l_tab_full );
      evolve.log_variable( 'p_drop_deps',        p_drop_deps );

      o_ev.change_action( 'determine partitioned table');

      -- find out which tables are partitioned
      l_src_part      := td_utils.is_part_table( p_source_owner, p_source_table);
      l_trg_part      := td_utils.is_part_table( p_owner, p_table );
      
      CASE
      -- raise exceptions if both are partitioned
        WHEN l_src_part AND l_trg_part
        THEN
          evolve.raise_err( 'both_part' );
      -- raise exceptions if neither are partitioned
        WHEN NOT l_src_part AND NOT l_trg_part
        THEN
          evolve.raise_err( 'neither_part' );
        ELSE
          NULL;
      END CASE;
           
      -- assign the appopriate tables and owners to the partitioned tags
      l_part_owner    := upper( CASE WHEN l_trg_part THEN p_owner ELSE p_source_owner END );
      l_part_table    := upper( CASE WHEN l_trg_part THEN p_table ELSE p_source_table END );

      l_nonpart_owner := upper( CASE WHEN l_trg_part THEN p_source_owner ELSE p_owner END );
      l_nonpart_table := upper( CASE WHEN l_trg_part THEN p_source_table ELSE p_table END );
      
      l_part_full     := upper( CASE WHEN l_trg_part THEN l_tab_full ELSE l_src_full END );
      l_nonpart_full  := upper( CASE WHEN l_trg_part THEN l_src_full ELSE l_tab_full END );
      
      -- get the partitioning type of the partitioned table
      l_part_type     := td_utils.get_tab_part_type( l_part_owner, l_part_table );

      evolve.log_variable('l_part_owner', l_part_full);
      evolve.log_variable('l_part_table', l_part_table);
      evolve.log_variable('l_nonpart_owner', l_nonpart_full);
      evolve.log_variable('l_nonpart_table', l_nonpart_table);

      evolve.log_variable('l_part_full', l_part_full);
      evolve.log_variable('l_nonpart_full', l_nonpart_full);

      evolve.log_variable('l_part_type', l_part_type);


      o_ev.change_action( 'check objects' );

      -- check to make sure the target table exists, and the partitioning is correct
      td_utils.check_table( p_owner => l_part_owner, 
                            p_table => l_part_table, 
                            p_partname => p_partname, 
                            p_partitioned => 'yes' );

      -- check to make sure the source table exists and the partitioning is correct
      td_utils.check_table( p_owner => l_nonpart_owner, 
                            p_table => l_nonpart_table, 
                            p_partitioned => 'no' );

      -- use either the value for P_PARTNAME or the max partition
      o_ev.change_action( 'get partition name' );
      
      IF p_partname IS NULL
      THEN

         SELECT DISTINCT last_value( partition_name ) 
                OVER ( partition BY table_owner, table_name 
                       ORDER BY partition_position ROWS BETWEEN unbounded preceding AND unbounded following )
           INTO l_partname
           FROM ( SELECT table_name,
                         CASE WHEN subpartition_name IS NULL THEN partition_position ELSE subpartition_position END partition_position,
                         table_owner,
                         CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                         CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                    FROM  all_tab_partitions ip
                    left JOIN all_tab_subpartitions isp
                         USING (table_owner, table_name, partition_name ))
          WHERE table_name = UPPER( l_part_table )
            AND table_owner = UPPER( l_part_owner );
         
      ELSE
         
         l_partname := p_partname;

      END IF;
      
      evolve.log_variable('l_partname', l_partname );      

      o_ev.change_action( 'manage statistics' );

      CASE
         -- we want to gather statistics
         -- we gather statistics first before the indexes are built
         -- the indexes will collect there own statistics when they are built
         -- that is why we don't cascade
         WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
         THEN
            gather_stats( p_owner        => p_source_owner,
                          p_segment      => p_source_table,
                          -- if the staging table is partitioned, then we need to specify the partition name
                          -- otherwise, we don't
                          p_partname     => CASE WHEN l_trg_part THEN NULL ELSE l_partname END,
                          p_percent      => p_statpercent,
                          p_degree       => p_statdegree,
                          p_method       => p_statmethod,
                          p_cascade      => 'no',
                          p_segment_type => 'table'
                        );

         -- we want to transfer the statistics from the current segment into the new segment
         -- this is preferable if automatic stats are handling stats collection
         -- and you want the load time not to suffer from statistics gathering
         WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
         THEN
            transfer_stats( p_owner                => p_source_owner,
                            p_segment              => p_source_table,
                            p_partname             => CASE WHEN l_trg_part THEN NULL ELSE l_partname END,
                            p_source_owner         => p_owner,
                            p_source_segment       => p_table,
                            p_source_partname      => CASE WHEN l_trg_part THEN l_partname ELSE NULL END,
                            p_segment_type         => 'table'
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
      -- indexes generate new statitics during the build
      -- that is why nothing is done for indexes during the statistics phase
      o_ev.change_action( 'build indexes' );
      build_indexes( p_owner             => p_source_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_part_type         => CASE WHEN l_trg_part THEN 'local' ELSE NULL END,
                     p_tablespace        => p_index_space,
                     p_concurrent        => p_idx_concurrency,
                     p_partname          => CASE
                                            WHEN p_index_space IS NOT NULL OR l_src_part
                                            THEN NULL
                                            ELSE l_partname
                                            END
                   );

      -- disable any unique constraints on the target table that are enforced with global indexes
      -- this is only if the target table is the partitioned one
      -- there are multiple reasons for this
      -- first off, there are lots of different errors that can occur because of this situation
      -- it would be difficult to handle and except all of them
      -- the other issue is that this just makes sense: the entire constraint would have to be revalidated anyway
      -- because the index it's based on is updated during the exchange
      IF l_trg_part
      THEN

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
			      p_queue_module	       => evolve.get_module,
                              p_queue_action           => 'enable constraints'
                            );
         END LOOP;
         
      END IF;

      -- build any constraints on the source table
      o_ev.change_action( 'build constraints' );
      build_constraints( p_owner             => p_source_owner,
                         p_table             => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_table      => p_table,
                         p_concurrent        => p_con_concurrency
                       );

      -- now exchange the table
      o_ev.change_action( 'exchange table' );

      -- have several exceptions that we want to handle when an exchange fails
      -- so we are using an EXIT WHEN loop
      -- if an exception that we handle is raised, then we want to rerun the exchange
      -- will try the exchange multiple times until it either succeeds, or an unrecognized exception is raised
      -- there is also an escape hatch built in: won't try the exchange more than 10 times
      LOOP
         l_retry_ddl    := FALSE;

         BEGIN
            evolve.exec_sql( p_sql       => 'alter table '
                                             || l_part_full 
                                             || ' exchange '
                                             ||l_part_type
                                             ||'ition '
                                             || l_partname
                                             || ' with table '
                                             || l_nonpart_full
                                             || ' including indexes without validation update global indexes',
                             p_auto      => 'yes'
                           );

            evolve.log_msg( l_nonpart_full || ' exchanged for partition ' || l_partname || ' of table ' || l_part_full );
            
            l_exit_cnt := l_exit_cnt + 1;

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
               evolve.exec_sql( p_sql => 'alter table ' || l_src_full || ' move compress', p_auto => 'yes' );
               evolve.log_msg( l_src_full || ' compressed to facilitate exchange', 3 );
               
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
                                  p_concurrent           => p_con_concurrency
                                );
            WHEN OTHERS
            THEN
               -- first log the error
               -- provide a backtrace from this exception handler to the next exception
               evolve.log_err;
               

               -- any constraints need to be enabled
               IF l_constraints
               THEN
		  o_ev.change_action( 'enable constraints' );
		  -- this statement will pull previously entered DDL statements off the queue and execute them
		  l_num_cons := dequeue_ddl( p_action => evolve.get_action,
					     p_module => evolve.get_module,
					     p_concurrent => p_con_concurrency );

		  -- log a message concerning number of constraints		  
		  evolve.log_msg(    l_num_cons
				  || ' constraint enablement process'
				  || CASE
                                  WHEN l_num_cons = 1
                                  THEN NULL
                                  ELSE 'es'
                                  END
				  || ' '
				  || CASE
                                  WHEN td_core.is_true( p_con_concurrency )
                                  THEN 'submitted to the Oracle scheduler'
                                  ELSE 'executed'
                                  END, 2
				);

               END IF;

               -- need to drop constraints and indexes if there is an exception
               -- this is for rerunability
               -- record the reason for the constraint and index drops
               -- only do this if P_DROP_DEPS is 'yes'
               
               -- drop the constraints on the staging table
               IF td_core.is_true( p_drop_deps )
               THEN

                  o_ev.change_action( 'drop constraints for rerun');
                  evolve.log_msg( 'Dropping constraints for restartability', 3 );
                  
                  drop_constraints( p_owner      => p_source_owner,
                                    p_table      => p_source_table
                                  );
               
                  -- drop indexes on the staging table
                  o_ev.change_action( 'drop indexes for rerun' );
                  evolve.log_msg( 'Dropping indexes for restartability', 3 );

                  drop_indexes( p_owner      => p_source_owner,
                                p_table      => p_source_table
                              );
                  
               END IF;

               o_ev.clear_app_info;
               RAISE;
         END;
         
         -- exit when we had a successful exchange
         -- or we tried 10 times without succeeding
         EXIT WHEN NOT l_retry_ddl OR l_exit_cnt = 10;
      END LOOP;

      -- any constraints need to be enabled
      IF l_constraints
      THEN
	 o_ev.change_action( 'enable constraints' );
	 -- this statement will pull previously entered DDL statements off the queue and execute them
		  l_num_cons := dequeue_ddl( p_action => evolve.get_action,
					     p_module => evolve.get_module,
					     p_concurrent => p_con_concurrency );

		  -- log a message concerning number of constraints		  
		  evolve.log_msg(    l_num_cons
				  || ' constraint enablement process'
				  || CASE
                                  WHEN l_num_cons = 1
                                  THEN NULL
                                  ELSE 'es'
                                  END
				  || ' '
				  || CASE
                                  WHEN td_core.is_true( p_con_concurrency )
                                  THEN 'submitted to the Oracle scheduler'
                                  ELSE 'executed'
                                  END, 2
				);

      END IF;
      
      -- only drop dependent objects if desired
      IF td_core.is_true( p_drop_deps )
      THEN
      
         -- drop constraints on the stage table
         evolve.log_msg( 'Dropping constraints on the staging table', 4 );
   
         BEGIN
            drop_constraints( p_owner => p_source_owner, p_table => p_source_table );
         EXCEPTION
            WHEN drop_iot_key
            THEN
               NULL;
         END;

         -- drop indexes on the staging table
         evolve.log_msg( 'Dropping indexes on the staging table', 4 );
         drop_indexes( p_owner => p_source_owner, p_table => p_source_table );
         
      END IF;

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
      l_temp_table   all_tables.table_name%TYPE   := 'TD$_RTBL' || TO_CHAR( SYSTIMESTAMP, 'mmddyyyyHHMISS' );
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
                       p_auto      => 'yes' );
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
      p_owner             VARCHAR2,
      p_table             VARCHAR2,
      p_source_table      VARCHAR2,
      p_tablespace        VARCHAR2 DEFAULT NULL,
      p_idx_concurrency   VARCHAR2 DEFAULT 'no',
      p_con_concurrency   VARCHAR2 DEFAULT 'no',
      p_statistics        VARCHAR2 DEFAULT 'transfer'
   )
   IS
      l_src_name       VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_source_table );
      l_tab_name       VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      l_rows           BOOLEAN        := FALSE;
      l_ddl            LONG;
      l_cnt        NUMBER;
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
      CASE
         WHEN REGEXP_LIKE( 'gather', p_statistics, 'i' )
      THEN
         -- we are gathering stats
         -- will be building indexes later, which gather their own statistics
         -- so P_CASCADE is no
         gather_stats( p_owner             => p_owner,
                       p_segment           => p_source_table,
                       p_cascade           => 'no',
                       p_segment_type      => 'table'
                     );
         WHEN REGEXP_LIKE( 'transfer', p_statistics, 'i' )
         -- we are transfering stats
         -- will be building indexes later, which gather their own statistics
         -- so P_CASCADE is no
      THEN

         transfer_stats( p_owner             => p_owner,
                         p_segment           => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_segment    => p_table,
                         p_segment_type      => 'table'
                       );
      -- if p_statistics is 'ignore', then do nothing

         ELSE NULL;
      END CASE;
      
      o_ev.change_action( 'build indexes' );
      -- build the indexes on the source table
      build_indexes( p_owner             => p_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_tablespace        => p_tablespace,
                     p_concurrent        => p_idx_concurrency,
      		     p_queue_module	 => evolve.get_module,
      		     p_queue_action	 => evolve.get_action
                   );

      -- build the constraints on the source table
      o_ev.change_action( 'build constraints' );
      build_constraints( p_owner             => p_owner,
                         p_table             => p_source_table,
                         p_source_owner      => p_owner,
                         p_source_table      => p_table,
                         p_basis             => 'all',
                         p_concurrent        => p_con_concurrency,
      		         p_queue_module	     => evolve.get_module,
      		         p_queue_action	     => evolve.get_action
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
      l_cnt := dequeue_ddl( p_action     => evolve.get_action,
			    p_module     => evolve.get_module );


      -- rename the constraints
      o_ev.change_action( 'rename constraints' );
      l_cnt := dequeue_ddl( p_action     => evolve.get_action,
			    p_module 	   => evolve.get_module);

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
      p_d_num           NUMBER DEFAULT 3,
      p_p_num           NUMBER DEFAULT 1048576,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL
   )
   IS
      l_partid          VARCHAR2( 30 )    := 'TD$' || SYS_CONTEXT( 'USERENV', 'SESSIONID' )
                                                   || TO_CHAR( SYSDATE, 'yyyymmdd_hhmiss' );

      l_tab_name   VARCHAR2( 61 )   := UPPER( p_owner )        || '.' || UPPER( p_table );
      l_src_name   VARCHAR2( 61 )   := UPPER( p_source_owner ) || '.' || UPPER( p_source_object );
      l_msg        VARCHAR2( 2000 );
      l_ddl        VARCHAR2( 2000 );
      l_pidx_cnt   NUMBER;
      l_idx_cnt    NUMBER;
      l_rows       BOOLEAN          DEFAULT FALSE;
      l_part_type  VARCHAR2(10)     := td_utils.get_tab_part_type( p_owner, p_table );
      o_ev         evolve_ot        := evolve_ot( p_module => 'unusable_indexes' );
   BEGIN

      CASE

      -- A partition name is passed in and either source_owner or source_object is passed in
      -- this used to cause an error
      -- this should now be allowed
      -- P_PARTNAME will just drive the process instead
      
      -- WHEN p_partname IS NOT NULL AND( p_source_owner IS NOT NULL OR p_source_object IS NOT NULL )
      -- THEN
            -- o_ev.clear_app_info;
            -- evolve.raise_err( 'parms_not_compatible', 'P_PARTNAME with either P_SOURCE_OWNER or P_SOURCE_OBJECT' );
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
      
      -- a partition name is passed
      -- or, a source table is passed
      -- this means that we want to only affect particular partitions on the target table
      -- but this should only work if the target table is actually partitioned
      IF td_utils.is_part_table(
                                 p_owner   => p_owner,
                                 p_table   => p_table
                               )
         AND (
               p_partname IS NOT NULL 
               OR p_source_object IS NOT NULL
             )
      THEN

         o_ev.change_action( 'populate PARTNAME table' );

         -- populate a global temporary table with the indexes to work on
         -- this is a requirement because the dynamic SQL needed to use the tbl$or$idx$part$num function
         populate_partname( p_owner              => p_owner,
                            p_table              => p_table,
                            p_partname           => p_partname,
                            p_source_owner       => p_source_owner,
                            p_source_object      => p_source_object,
                            p_source_column      => p_source_column,
                            p_partid             => l_partid,
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
                                     ELSE ' modify '
                                     || part_type
                                     ||'ition ' || partition_name
                                     END
                                     || ' unusable' DDL,
                                     
                                     'Index '
                                     || CASE idx_ddl_type
                                     WHEN 'I'
                                     THEN NULL
                                     ELSE part_type
                                     ||'ition ' || partition_name || ' of '
                                     END
                                     || owner
                                     || '.'
                                     || index_name
                                     || ' altered to unusable' msg,
                                     idx_ddl_type, partition_name,
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
                                     END status, include, partition_position,
                                     part_type
                                FROM ( SELECT partition_position, index_type, owner, ai.index_name, partition_name,
                                              partitioned, aip.status aip_status,
                                              ai.status ai_status,
                                              part_type,
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
                                         FROM td_part_gtt JOIN 
                                              (  SELECT DISTINCT CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                                                        index_owner,
                                                        index_name,
                                                        CASE WHEN subpartition_name IS null THEN ip.status ELSE isp.status END status,
                                                        CASE WHEN subpartition_name IS null THEN 'part' ELSE 'subpart' END part_type
                                                   FROM all_ind_partitions ip
                                                   left JOIN all_ind_subpartitions isp
                                                        USING (index_owner, index_name, partition_name)
                                              ) aip USING( partition_name )
                                        RIGHT JOIN all_indexes ai
                                              ON ai.index_name = aip.index_name AND ai.owner = aip.index_owner
                                        WHERE ai.table_name = UPPER( p_table )
                                          AND ai.table_owner = UPPER( p_owner )
                                          AND partid = l_partid)
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
                               ORDER BY idx_ddl_type )
                      WHERE status IN( 'VALID', 'USABLE', 'N/A' ))
      LOOP
         o_ev.change_action( 'execute index DDL' );
         l_rows        := TRUE;
         evolve.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes' );
         evolve.log_msg( c_idx.msg, 3);
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
         evolve.log_msg( 'No matching usable indexes found on ' || l_tab_name, 1 );
      END IF;

      DELETE FROM td_part_gtt WHERE partid = l_partid;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END unusable_indexes;

   -- rebuilds all unusable index segments on a particular table
   PROCEDURE usable_indexes( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL,
      p_concurrent      VARCHAR2 DEFAULT 'no' 
   )
   IS
      l_ddl             VARCHAR2( 2000 );
      l_rows            BOOLEAN          := FALSE;                                            -- to catch empty cursors
      l_cnt             NUMBER           := 0;
      l_tab_name        VARCHAR2( 61 )   := UPPER( p_owner || '.' || p_table );
      l_concurrent_id   VARCHAR2( 100 );
      l_part_type       VARCHAR2( 10 )   := td_utils.get_tab_part_type( p_owner, p_table );
      o_ev              evolve_ot        := evolve_ot( p_module => 'usable_indexes' );
   BEGIN

      evolve.log_variable( 'l_part_type', l_part_type );
            
      -- make sure the table exists
      td_utils.check_table( p_owner => p_owner, p_table => p_table );
      
      IF l_part_type <> 'normal' OR p_part_type IS NULL OR lower( p_part_type ) IN ('local','all')
      THEN
         
         o_ev.change_action( 'process local indexes');

         IF td_core.is_true( p_concurrent )
         THEN
            -- need to get a unique "job header" number in case we are running concurrently
            o_ev.change_action( 'get concurrent id' );

            l_concurrent_id    := evolve.get_concurrent_id;
            evolve.log_variable( 'l_concurrent_id', l_concurrent_id );
         END IF;
         
         o_ev.change_action( 'rebuild local indexes' );
         FOR c_idx IN ( SELECT  DISTINCT table_name, partition_position,
                               'alter table '
                               || table_owner
                               || '.'
                               || table_name
                               || ' modify '
                               || CASE part_type WHEN 'subpart' THEN 'subpartition ' ELSE 'partition ' end
                               || partition_name
                               || ' rebuild unusable local indexes' DDL,
                               partition_name,
                               part_type
                          FROM ( SELECT table_name,
                                        CASE WHEN subpartition_name IS NULL THEN partition_position ELSE subpartition_position END partition_position,
                                        table_owner,
                                        CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                                        CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                                   FROM  all_tab_partitions ip
                                   left JOIN all_tab_subpartitions isp
                                        USING (table_owner, table_name, partition_name )) tabs 
                          JOIN ( SELECT table_name,
                                        table_owner,
                                        index_type,
                                        ip.index_name,
                                        CASE WHEN subpartition_name IS NULL THEN ip.partition_name ELSE isp.subpartition_name END partition_name,
                                        CASE WHEN subpartition_name IS NULL THEN ip.status ELSE isp.status END status
                                   FROM all_indexes ix
                                   JOIN all_ind_partitions ip
                                        ON ix.owner = ip.index_owner
                                    AND ix.index_name = ip.index_name
                                   left JOIN all_ind_subpartitions isp
                                        ON ip.index_owner = isp.index_owner
                                    AND ip.index_name = isp.index_name
                                    AND ip.partition_name = isp.partition_name ) inds
                               USING (table_owner, table_name, partition_name)
                         WHERE table_name = UPPER( p_table ) 
                           AND table_owner = UPPER( p_owner )
                           AND REGEXP_LIKE( partition_name, nvl( p_partname, '.' ), 'i' )
                           AND REGEXP_LIKE( index_type, nvl( p_index_type, '.' ), 'i' )
                           AND REGEXP_LIKE( index_name, nvl( p_index_regexp, '.' ), 'i' )
                           AND status = 'UNUSABLE'
                         ORDER BY table_name, partition_position
                      )
         LOOP
            evolve.exec_sql( p_sql => c_idx.DDL, p_auto => 'yes', p_concurrent_id => l_concurrent_id );
            evolve.log_msg(    'Unusable indexes on '
                            || l_part_type || 'ition '
                            || c_idx.partition_name
                            || ' of table '
                            || l_tab_name
                            || ' rebuilt'
                            , 3
                          );

            l_cnt    := l_cnt + 1;
         END LOOP;
         
         evolve.log_variable( 'L_CNT', l_cnt );
         
         IF l_cnt = 0
         THEN
            evolve.log_msg( 'No matching unusable local indexes found', 1 );
         ELSE

            evolve.log_msg(    'Rebuild processes for unusable indexes on '
                            || l_cnt
                            || ' '
                            || l_part_type || 'ition'
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


      IF l_part_type = 'normal' OR p_part_type IS NULL OR lower( p_part_type ) IN ('global','all')
      THEN

         -- now see if any global are still unusable
         o_ev.change_action( 'process global indexes' );

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
                            || 'indexes found', 1
                          );
         END IF;
         
      END IF;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END usable_indexes;

   PROCEDURE transfer_stats(
      p_owner             VARCHAR2,
      p_segment           VARCHAR2,
      p_source_owner      VARCHAR2,
      p_source_segment    VARCHAR2,
      p_partname          VARCHAR2 DEFAULT NULL,
      p_source_partname   VARCHAR2 DEFAULT NULL,
      p_segment_type      VARCHAR2 DEFAULT NULL
   )
   IS
      -- generate statid for the OPT_STATS table
      l_statid            VARCHAR2( 30 )    := 'TD$' || SYS_CONTEXT( 'USERENV', 'SESSIONID' )
      || TO_CHAR( SYSDATE, 'yyyymmdd_hhmiss' );
      
      -- variables for the segment_types
      l_src_seg_type       dba_segments.segment_type%TYPE;
      l_trg_seg_type       dba_segments.segment_type%TYPE;
      
      -- variables to know what type of partitioning is used
      l_src_part_type     VARCHAR2(20);
      l_trg_part_type     VARCHAR2(20);
      
      -- variables to hold qualified segments
      l_source_seg        VARCHAR2(61) := upper( p_source_owner||'.'||p_source_segment );
      l_target_seg        VARCHAR2(61) := upper( p_owner||'.'||p_segment );
      
      -- partition names for when p_partname is a subpartition
      l_src_part_name     all_tab_partitions.partition_name%TYPE;
      l_trg_part_name     all_tab_partitions.partition_name%TYPE;
      
      -- number of segments
      l_src_num_segs      NUMBER;
      l_trg_num_segs      NUMBER;
      
      -- translate number of segments to a Boolean
      l_src_global        BOOLEAN;
      l_trg_global        BOOLEAN;

      e_no_stats    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      l_rows        BOOLEAN        := FALSE;                                                  -- to catch empty cursors
      o_ev          evolve_ot      := evolve_ot( p_module => 'transfer_stats' );
   BEGIN

      evolve.log_variable('p_source_partname',p_source_partname);      
      -- partition type
      -- get source segment type
      -- set number of segments
      BEGIN
         SELECT nvl(part_type,'normal'),
                segment_type,
                count(*)
           INTO l_src_part_type,
                l_src_seg_type,
                l_src_num_segs
           FROM (  SELECT table_name segment_name,
                          owner,
                          'table' segment_type,
                          lower(partitioned) partitioned
                     FROM all_tables
                    UNION
                   SELECT index_name segment_name,
                          owner,
                          'index' segment_type,
                          lower(partitioned) partitioned
                     FROM all_indexes )
           left JOIN  ( SELECT table_name segment_name,
                               table_owner owner,
                               'table' segment_type,
                               CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                               CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                          FROM  all_tab_partitions ip
                          left JOIN all_tab_subpartitions isp
                               USING (table_owner, table_name, partition_name)
                         UNION
                        SELECT index_name segment_name,
                               index_owner owner,
                               'index' segment_type,
                               CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                               CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                          FROM all_ind_partitions ip
                          left JOIN all_ind_subpartitions isp
                               USING (index_owner, index_name, partition_name)
                      )
                USING (owner, segment_name, segment_type)
          WHERE segment_name = upper( p_source_segment )
            AND owner = upper( p_source_owner )
            AND REGEXP_LIKE( segment_type, NVL( p_segment_type, '.' ), 'i' )
            AND REGEXP_LIKE( NVL(partition_name,'~'), NVL( p_source_partname, '.' ), 'i' )
          GROUP BY part_type,segment_type;
      EXCEPTION
         WHEN no_data_found
         THEN
            CASE
            WHEN p_source_partname IS NOT NULL
            THEN            
               evolve.raise_err( 'no_part', upper( p_source_partname )||' of '||l_source_seg );
            ELSE
               evolve.raise_err( 'no_segment', l_source_seg );
            END CASE;
         WHEN too_many_rows
         THEN
            evolve.raise_err( 'multiple_segments', l_source_seg );
      END;

      evolve.log_variable( 'l_src_seg_type',l_src_seg_type );
      evolve.log_variable( 'l_src_part_flg',l_src_part_type );      
      evolve.log_variable( 'l_src_seg_type',l_src_seg_type );

      -- use number of segments to determine whether we use global stats      
      l_src_global  := CASE WHEN l_src_num_segs > 1 THEN TRUE WHEN l_src_num_segs = 1 THEN FALSE END;
      evolve.log_variable( 'l_src_global', l_src_global );

      -- partition type
      -- get source segment type
      -- set number of segments
      BEGIN
         SELECT nvl(part_type,'normal'),
                segment_type,
                count(*)
           INTO l_trg_part_type,
                l_trg_seg_type,
                l_trg_num_segs
           FROM (  SELECT table_name segment_name,
                          owner,
                          'table' segment_type,
                          lower(partitioned) partitioned
                     FROM all_tables
                    UNION
                   SELECT index_name segment_name,
                          owner,
                          'index' segment_type,
                          lower(partitioned) partitioned
                     FROM all_indexes )
           left JOIN  ( SELECT table_name segment_name,
                               table_owner owner,
                               'table' segment_type,
                               CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                               CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                          FROM  all_tab_partitions ip
                          left JOIN all_tab_subpartitions isp
                               USING (table_owner, table_name, partition_name)
                         UNION
                        SELECT index_name segment_name,
                               index_owner owner,
                               'index' segment_type,
                               CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                               CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
                          FROM all_ind_partitions ip
                          left JOIN all_ind_subpartitions isp
                               USING (index_owner, index_name, partition_name)
                      )
                USING (owner, segment_name, segment_type)
          WHERE segment_name = upper( p_segment )
            AND owner = upper( p_owner )
            AND REGEXP_LIKE( segment_type, NVL( p_segment_type, '.' ), 'i' )
            AND REGEXP_LIKE( NVL(partition_name,'~'), NVL( p_partname, '.' ), 'i' )
          GROUP BY part_type,segment_type;
      EXCEPTION
         WHEN no_data_found
         THEN
            CASE
            WHEN p_source_partname IS NOT NULL
            THEN            
               evolve.raise_err( 'no_part', upper( p_partname )||' of '||l_target_seg );
            ELSE
               evolve.raise_err( 'no_segment', l_target_seg );
            END CASE;
         WHEN too_many_rows
         THEN
            evolve.raise_err( 'multiple_segments', l_target_seg );
      END;

      
      evolve.log_variable( 'l_trg_seg_type',l_trg_seg_type );
      evolve.log_variable( 'l_trg_part_type',l_trg_part_type );
      evolve.log_variable( 'l_trg_num_segs',l_trg_num_segs );
            
      -- use number of segments to determine whether we use global stats      
      l_trg_global  := CASE WHEN l_trg_num_segs > 1 THEN TRUE WHEN l_trg_num_segs = 1 THEN FALSE END;
      evolve.log_variable( 'l_trg_global', l_trg_global );
      

      -- this will either take partition level statistics and import into a table
      -- or, it will take table level statistics and import it into a partition
      -- or, it will take table level statistics and import it into a table.
      IF l_src_seg_type = 'table'
      THEN
         
         o_ev.change_action( 'export table stats' );
         DBMS_STATS.export_table_stats( ownname       => p_source_owner,
                                        tabname       => p_source_segment,
                                        partname      => p_source_partname,
                                        statown       => USER,
                                        stattab       => 'OPT_STATS',
                                        statid        => l_statid
                                      );
      ELSIF l_src_seg_type = 'index'
      THEN

         o_ev.change_action( 'export index stats' );
         DBMS_STATS.export_index_stats( ownname       => p_source_owner,
                                        indname       => p_source_segment,
                                        partname      => p_source_partname,
                                        statown       => USER,
                                        stattab       => 'OPT_STATS',
                                        statid        => l_statid
                                      );
      ELSE
         evolve.raise_err('seg_not_supported',l_src_seg_type);
      END IF;
      
      
      -- if this is a subpartition, then we need the partition name
      IF l_src_part_type = 'subpart'
      THEN

         l_src_part_name := td_utils.get_part_for_subpart( p_owner, p_segment, p_source_partname, l_src_seg_type );
         evolve.log_variable( 'l_src_part_name',l_src_part_name );

      END IF;

      -- if this is a subpartition, then we need the partition name
      IF l_trg_part_type = 'subpart'
      THEN

         l_trg_part_name := td_utils.get_part_for_subpart( p_owner, p_segment, p_partname, l_trg_seg_type );
         evolve.log_variable( 'l_trg_part_name',l_trg_part_name );

      END IF;

      -- now, update the table name in the stats table to the new table name
      UPDATE opt_stats
         SET c1 = UPPER( p_segment )
       WHERE statid = l_statid;
      
      -- update the owner to the new owner
      UPDATE opt_stats
         SET c5 = UPPER( p_owner )
       WHERE statid = l_statid;
      
      -- now, we'll perform a few operations based on the source and target information
      CASE 

      -- CASE 1
      -- we have differing partition types (one subpart, one part)
      WHEN (( l_src_part_type = 'subpart' AND l_trg_part_type = 'part' )
             OR ( l_src_part_type = 'part' AND l_trg_part_type = 'subpart'))
      
      -- and we also have multiple segments brought in
      -- this means we have global and segment level statistics... multiple rows
      AND ( l_src_global AND l_trg_global)

      THEN
      -- we can't move global stats from partitioned table to subpartitioned table, and vice versa
      -- just not supported
      evolve.log_msg('Stats CASE 1 entered',5);
      evolve.raise_err( 'incompatible_part_type' );
      
         
      -- CASE 2
      -- we're moving multiple rows to multiple rows
      -- this will include table and partition level statistics
      WHEN ( l_src_global AND l_trg_global ) 


      -- we know that they are of the same partition type because it passed CASE 1
      -- we also know there are the same number of segments
      AND ( l_trg_num_segs = l_src_num_segs )

      THEN 
         evolve.log_msg('Stats CASE 2 entered',5);

      -- we'll assume that the partitioning structure is the same for both tables
      -- if the partition names are different, than this is not supported
      -- that's on the end user to guarantee
      -- so there is really nothing to do here
         

      -- CASE 3
      -- we know there are multiple rows in the source
      -- we know that the target cannot accept multiple rows
      WHEN l_src_global AND NOT l_trg_global
            
      -- we also know that the partition type of the target is subpartitioning
      AND l_trg_part_type='subpart'
            
      THEN

         evolve.log_msg('Stats CASE 3 entered',5);

      -- we need to delete all the source rows specific to partitioning            
         DELETE FROM opt_stats
          WHERE statid = l_statid AND( c2 IS NOT NULL OR c3 IS NOT NULL );
            
         -- we also need to set update the partitioning and subpartitioning columns for the remaining single row
         UPDATE opt_stats
            SET c2 = l_trg_part_name,
                c3 = p_partname
          WHERE statid = l_statid;

      -- CASE 4
      -- we know there are multiple rows in the source
      -- we know that the target cannot accept multiple rows
      WHEN l_src_global AND NOT l_trg_global
            
         -- we also know that the partition type of the target is regular partitioning
         AND l_trg_part_type = 'part'
            
      THEN

         evolve.log_msg('Stats CASE 4 entered',5);

         -- we need to delete all the source rows specific to partitioning            
         DELETE FROM opt_stats
          WHERE statid = l_statid AND( c2 IS NOT NULL OR c3 IS NOT NULL );
            
         -- we also need to update the partitioning column for the remaining single row
         UPDATE opt_stats
            SET c2 = p_partname,
                c3 = null
          WHERE statid = l_statid;
         
      -- CASE 5
      -- we know there are multiple rows in the source
      -- we know that the target cannot accept multiple rows
      WHEN l_src_global AND NOT l_trg_global
            
         -- we also know that the partition type of the target is regular partitioning
         AND l_trg_part_type = 'normal'
            
      THEN

         evolve.log_msg('Stats CASE 5 entered',5);

         -- we need to delete all the source rows specific to partitioning            
         DELETE FROM opt_stats
          WHERE statid = l_statid AND( c2 IS NOT NULL OR c3 IS NOT NULL );
            
      -- CASE 6
      -- we know there is a single row in the source
      -- we know that the target can handle a single row( table level ) or multiple rows (partition level)            
      WHEN NOT l_src_global AND l_trg_global
         
      -- so we really don't have to do anything
      -- we'll bring in the single row into the target
      THEN

         evolve.log_msg('Stats CASE 6 entered',5);
         
         -- we also need to update the partitioning column for the remaining single row
         UPDATE opt_stats
            SET c2 = UPPER( CASE l_trg_part_type WHEN 'subpart' THEN l_trg_part_name ELSE p_partname END ),
                c3 = UPPER( CASE l_trg_part_type WHEN 'subpart' THEN p_partname ELSE null END )
          WHERE statid = l_statid;


      -- CASE 7
      -- we know there is a single row in the source
      -- we know that there is a single row in the target
      WHEN NOT l_src_global AND NOT l_trg_global

      -- however, we also know that the source is a partition            
         AND p_source_partname IS NOT NULL 

      -- we also know that the target is a table            
         AND p_partname IS NULL 

      -- so we need to set both columns C2 and C3 to null       
      THEN

         evolve.log_msg('Stats CASE 7 entered',5);
         
         -- we need to update the partitioning column for the remaining single row
         UPDATE opt_stats
            SET c2 = NULL,
                c3 = NULL
          WHERE statid = l_statid;


      -- CASE 8
      -- we know there is a single row in the source
      -- we know that there is a single row in the target
      WHEN NOT l_src_global AND NOT l_trg_global

      -- however, we also know that the source is a table            
         AND p_source_partname IS NULL 

      -- we also know that the target is a partition            
         AND p_partname IS NOT NULL 

      -- so we need to update columns C2 and C3 in the stats table
      -- with the partition and subpartition (if applicable) 
      THEN

         evolve.log_msg('Stats CASE 8 entered',5);
         
         -- we need to update the partitioning column for the remaining single row
         UPDATE opt_stats
            SET c2 = UPPER( CASE l_trg_part_type WHEN 'subpart' THEN l_trg_part_name ELSE p_partname END ),
                c3 = UPPER( CASE l_trg_part_type WHEN 'subpart' THEN p_partname ELSE null END )
          WHERE statid = l_statid;
               
               
      ELSE
               
          evolve.log_msg('No Stats CASE entered',5);

      END CASE;

      
      IF NOT evolve.is_debugmode
      THEN

         -- now, import the segment statistics
         IF REGEXP_LIKE( l_src_seg_type, 'table','i' )
         THEN
            
            o_ev.change_action( 'import table stats' );
            
            DBMS_STATS.import_table_stats( ownname       => p_owner,
                                           tabname       => p_segment,
                                           partname      => p_partname,
                                           statown       => USER,
                                           stattab       => 'OPT_STATS',
                                           statid        => l_statid
                                         );
         ELSE

            o_ev.change_action( 'import index stats' );

            DBMS_STATS.import_index_stats( ownname       => p_owner,
                                           indname       => p_segment,
                                           partname      => p_partname,
                                           statown       => USER,
                                           stattab       => 'OPT_STATS',
                                           statid        => l_statid
                                         );

         END IF;

      END IF;


      -- now, delete these records from the stats table
      IF NOT evolve.is_debugmode
      THEN

         DELETE FROM opt_stats
          WHERE statid = l_statid;
         
      END IF;

      evolve.log_msg(    'Statistics from '
                      || CASE 
                         WHEN p_source_partname IS NOT NULL 
                         THEN l_src_part_type
                              || 'ition '
                              || upper( p_source_partname )
                              || ' of '
                         ELSE NULL
                         END                        
                      || UPPER( l_source_seg )
                      || ' transferred to '
                      || CASE 
                         WHEN p_partname IS NOT NULL 
                         THEN l_trg_part_type
                              || 'ition '
                              || upper( p_partname )
                              || ' of '
                         ELSE NULL
                         END                        
                      || UPPER( l_target_seg )
                     );

           
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END transfer_stats;

   PROCEDURE gather_stats(
      p_owner             VARCHAR2,
      p_segment           VARCHAR2,
      p_partname          VARCHAR2 DEFAULT NULL,
      p_percent           NUMBER   DEFAULT NULL,
      p_degree            NUMBER   DEFAULT NULL,
      p_method            VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_granularity       VARCHAR2 DEFAULT 'AUTO',
      p_cascade           VARCHAR2 DEFAULT NULL,
      p_segment_type      VARCHAR2 DEFAULT NULL
   )
   IS
      -- generate statid for the OPT_STATS table
      l_statid            VARCHAR2( 30 )    := 'TD$' || SYS_CONTEXT( 'USERENV', 'SESSIONID' )
      || TO_CHAR( SYSDATE, 'yyyymmdd_hhmiss' );
      
      -- variables for the segment_types
      l_target_type       dba_segments.segment_type%TYPE;
      
      -- variables to know whether the tables are partitioned or not
      l_trg_part_flg      VARCHAR2(3);
      
      -- variables to hold qualified segments
      l_target_seg        VARCHAR2(61) := p_owner||'.'||p_segment;

      e_no_stats    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_stats, -20000 );
      l_rows        BOOLEAN        := FALSE;                                                  -- to catch empty cursors
      o_ev          evolve_ot      := evolve_ot( p_module => 'gather_stats' );
   BEGIN

      BEGIN
         SELECT segment_type,
                partitioned
           INTO l_target_type,
                l_trg_part_flg
           FROM (
                  SELECT table_name segment_name,
                         owner,
                         'table' segment_type,
                         lower(partitioned) partitioned
                    FROM all_tables
                   UNION
                  SELECT index_name segment_name,
                         owner,
                         'index' segment_type,
                         lower(partitioned) partitioned
                    FROM all_indexes
                )
          WHERE owner = upper(p_owner)
            AND segment_name = upper(p_segment)
            AND REGEXP_LIKE( NVL(segment_type,'~'), NVL( p_segment_type, '.' ), 'i' );
      EXCEPTION
         WHEN no_data_found
         THEN
            evolve.raise_err( 'no_segment', l_target_seg );
         WHEN too_many_rows
         THEN
            evolve.raise_err( 'multiple_segments', l_target_seg );
      END;

      -- register variable values
      evolve.log_variable( 'l_target_type', l_target_type );
      evolve.log_variable( 'l_trg_part_flg', l_trg_part_flg );
      
      
      -- raise an exception if we expect a partitioned table but didn't get one
      IF p_partname IS NOT NULL
         AND l_trg_part_flg = 'no'
      THEN
         
         evolve.raise_err( 'no_part', p_partname );
         
      END IF;
      

      IF NOT evolve.is_debugmode
      THEN
      
         -- this will either take partition level statistics and import into a table
         -- or, it will take table level statistics and import it into a partition
         -- or, it will take table level statistics and import it into a table.
         IF REGEXP_LIKE( l_target_type, 'table','i' )
         THEN
            
	    o_ev.change_action( 'gathering table stats' );
            DBMS_STATS.gather_table_stats( ownname               => p_owner,
                                           tabname               => p_segment,
                                           partname              => p_partname,
                                           estimate_percent      => NVL( p_percent, DBMS_STATS.auto_sample_size ),
                                           method_opt            => p_method,
                                           DEGREE                => NVL( p_degree, DBMS_STATS.auto_degree ),
                                           granularity           => p_granularity,
                                           CASCADE               => NVL( td_core.is_true( p_cascade, TRUE ),
                                                                         DBMS_STATS.auto_cascade
                                                                       )
                                         );
         ELSE

            o_ev.change_action( 'export index stats' );
            DBMS_STATS.gather_index_stats( ownname               => p_owner,
                                           indname               => p_segment,
                                           partname              => p_partname,
                                           estimate_percent      => NVL( p_percent, DBMS_STATS.auto_sample_size ),
                                           DEGREE                => NVL( p_degree, DBMS_STATS.auto_degree ),
                                           granularity           => p_granularity
                                         );
         END IF;
         
      END IF;


      evolve.log_msg(    'Statistics gathered on '
                      || CASE
                           WHEN p_partname IS NULL
                             THEN NULL
                           ELSE 'partition ' || UPPER( p_partname ) || ' of segment '
                         END
                      || UPPER( l_target_seg ) );

           
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END gather_stats;

   -- procedure to add a partition (and possible subpartition) to a range partitioned table (with possible list underneath)
   PROCEDURE add_range_part( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_value           VARCHAR2,
      p_tablespace      VARCHAR2 DEFAULT NULL,
      p_compress        VARCHAR2 DEFAULT 'no'
   )
   IS
      l_ddl             LONG;
      l_tab_name        VARCHAR2( 61 )   := UPPER( p_owner || '.' || p_table );
      l_part_type       VARCHAR2( 10 )   := td_utils.get_tab_part_type( p_owner, p_table );
      l_higher_part     all_tab_partitions.partition_name%type;

      e_higher_part     EXCEPTION;
      PRAGMA            EXCEPTION_INIT( e_higher_part, -14074 );

      o_ev              evolve_ot        := evolve_ot( p_module => 'add_range_part' );
   BEGIN

      evolve.log_variable( 'l_part_type', l_part_type );
      
      -- basic checks about the table to make sure all is well
      CASE
      WHEN l_part_type = 'normal'
      THEN
         evolve.raise_err( 'not_partioned', l_tab_name );
      ELSE
         NULL;
      END CASE;
      
      -- try to construct an ADD statement from the parameters provided
      l_ddl := 'alter table '
               || l_tab_name
               || ' add partition '
               || upper( p_partname )
               || ' values less than ('
               || p_value
               || ') '
               || CASE WHEN td_core.is_true( p_compress )
                  THEN NULL 
                  ELSE 'no' END
               || 'compress '
               || CASE             
                  WHEN p_tablespace is NULL 
                  THEN NULL
                  ELSE ' tablespace '|| upper( p_tablespace )
                  END
               || '( subpartition '
               || upper( p_partname )
               || '_DEFAULT values (DEFAULT) '
               || CASE             
                  WHEN p_tablespace is NULL 
                  THEN NULL
                  ELSE ' tablespace '|| upper( p_tablespace )
                  END
               || ' )';
                 
      BEGIN
         
         evolve.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
         
      EXCEPTION
         WHEN e_higher_part
         THEN
            
            -- log that the exception was handled
            evolve.log_exception( 'e_higher_part' );
            
            -- find out whether the particular partition has a default subpartition
            SELECT DISTINCT last_value( partition_name ) 
                     OVER ( partition BY table_owner, table_name 
                            ORDER BY partition_position ROWS BETWEEN unbounded preceding AND unbounded following )
              INTO l_higher_part
              FROM all_tab_partitions
             WHERE table_name = UPPER( p_table )
               AND table_owner = UPPER( p_owner );
            
            evolve.log_variable( 'l_higer_part', l_higher_part );
  
            -- try to construct a SPLIT statement from the parameters provided
            l_ddl := 'alter table '
            || l_tab_name
            || ' split partition '
            || l_higher_part
            || ' at ('
            || p_value
            || ') into ( partition '
            ||p_partname
            || CASE 
               WHEN p_tablespace is NULL 
               THEN NULL
               ELSE ' tablespace '||p_tablespace
               END
            || ', partition '
            || l_higher_part
            || ')';
            
            evolve.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
               
      END;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END add_range_part;
   
   -- procedure to add a partition (and possible subpartition) to a range partitioned table (with possible list underneath)
   PROCEDURE add_range_list_subpart( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_subpartname     VARCHAR2,
      p_value           VARCHAR2,
      p_tablespace      VARCHAR2 DEFAULT NULL,
      p_compress        VARCHAR2 DEFAULT 'no'
   )
   IS
      l_ddl             LONG;
      l_tab_name        VARCHAR2( 61 )   := UPPER( p_owner || '.' || p_table );
      l_part_type       VARCHAR2( 10 )   := td_utils.get_tab_part_type( p_owner, p_table );
      l_default_part    all_tab_subpartitions.subpartition_name%TYPE;
      
      e_default_part    EXCEPTION;
      PRAGMA            EXCEPTION_INIT( e_default_part, -14621 );

      o_ev              evolve_ot        := evolve_ot( p_module => 'add_range_part' );
   BEGIN

      evolve.log_variable( 'l_part_type', l_part_type );
      
      -- make sure this is a sbupartitioned table
      CASE
      WHEN l_part_type <> 'subpart'
      THEN
         evolve.raise_err( 'not_subpartitioned', l_tab_name );
      ELSE
         NULL;
      END CASE;
      
      -- try to construct an ADD statement from the parameters provided
      l_ddl := 'alter table '
               || l_tab_name
               || ' modify partition '
               || p_partname
               || ' add subpartition '
               || p_subpartname 
               || ' values ('
               || p_value
               || ')'
               || CASE 
                  WHEN p_tablespace is NULL 
                  THEN NULL
                  ELSE ' tablespace '||p_tablespace
                  END;
 
                 
      BEGIN
         
         evolve.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
         
      EXCEPTION
         WHEN e_default_part
         THEN
            
            -- log that the exception was handled
            evolve.log_exception( 'e_default_part' );
            
            -- find out whether the particular partition has a default subpartition

            SELECT DISTINCT last_value( subpartition_name ) 
                     OVER ( partition BY table_owner, table_name, partition_name 
                            ORDER BY subpartition_position ROWS BETWEEN unbounded preceding AND unbounded following )
              INTO l_default_part
              FROM all_tab_subpartitions
             WHERE table_name = UPPER( p_table )
               AND table_owner = UPPER( p_owner )
               AND partition_name = upper( p_partname );
            
            evolve.log_variable( 'l_default_part', l_default_part );
  
            -- try to construct a SPLIT statement from the parameters provided
            l_ddl := 'alter table '
            || l_tab_name
            || ' split subpartition '
            || l_default_part
            || ' values ('
            || p_value
            || ') into ( subpartition '
            ||p_subpartname
            || CASE 
               WHEN p_tablespace is NULL 
               THEN NULL
               ELSE ' tablespace '||p_tablespace
               END
            || ', subpartition '
            || l_default_part
            || ')';
            
            evolve.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
               
      END;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END add_range_list_subpart;

   PROCEDURE partition_action( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_action          VARCHAR2 DEFAULT 'truncate'
   )
   IS
      l_ddl             LONG;
      l_tab_name        VARCHAR2( 61 )   := UPPER( p_owner || '.' || p_table );
      l_part_type       VARCHAR2( 10 )   := td_utils.get_tab_part_type( p_owner, p_table, p_partname );
      l_higher_part     all_tab_partitions.partition_name%type;

      o_ev              evolve_ot        := evolve_ot( p_module => 'partition_action' );
   BEGIN

      evolve.log_variable( 'l_part_type', l_part_type );
            
      -- construct a TRUNCATE statement from the parameters provided
      l_ddl := 'alter table '
               || l_tab_name
               || ' '
               || p_action 
               || ' '
               || l_part_type
               || 'ition '
               || upper( p_partname );
                 
      BEGIN
         
         evolve.exec_sql( p_sql => l_ddl, p_auto => 'yes' );
         
      END;

      evolve.log_msg( regexp_replace(l_part_type,'^.',upper(regexp_substr(l_part_type,'^.')))
                      || 'ition '
                      || upper( p_partname )
                      || ' '
                      || p_action
                      || CASE p_action 
                         WHEN 'drop' THEN 'ped'
                         WHEN 'truncate' THEN 'd'
                         ELSE 'ed'
                         END );

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END partition_action;

END td_dbutils;
/

SHOW errors