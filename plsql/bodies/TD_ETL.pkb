CREATE OR REPLACE PACKAGE BODY td_etl
AS
   PROCEDURE truncate_table(
      p_owner   VARCHAR2,
      p_table   VARCHAR2,
      p_reuse   VARCHAR2 DEFAULT 'no'
   )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      td_ddl.truncate_table( p_owner      => p_owner, p_table => p_table,
                             p_reuse      => p_reuse );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END truncate_table;

   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2
            DEFAULT 'yes' )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'truncate_table' );
   BEGIN
      td_ddl.drop_table( p_owner => p_owner, p_table => p_table, p_purge => p_purge );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END drop_table;

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
      o_ev   evolve_ot := evolve_ot( p_module => 'build_table' );
   BEGIN
      td_ddl.build_table( p_owner             => p_owner,
                          p_table             => p_table,
                          p_source_owner      => p_source_owner,
                          p_source_table      => p_source_table,
                          p_tablespace        => p_tablespace,
                          p_partitioning      => p_partitioning,
                          p_rows              => p_rows,
                          p_statistics        => p_statistics
                        );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END build_table;

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
      o_ev   evolve_ot := evolve_ot( p_module => 'build_indexes' );
   BEGIN
      td_ddl.build_indexes( p_owner             => p_owner,
                            p_table             => p_table,
                            p_source_owner      => p_source_owner,
                            p_source_table      => p_source_table,
                            p_index_regexp      => p_index_regexp,
                            p_index_type        => p_index_type,
                            p_part_type         => p_part_type,
                            p_tablespace        => p_tablespace,
                            p_partname          => p_partname
                          );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
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
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
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
      o_ev   evolve_ot := evolve_ot( p_module => 'build_constraints' );
   BEGIN
      td_ddl.build_constraints( p_owner                  => p_owner,
                                p_table                  => p_table,
                                p_source_owner           => p_source_owner,
                                p_source_table           => p_source_table,
                                p_constraint_type        => p_constraint_type,
                                p_constraint_regexp      => p_constraint_regexp,
                                p_seg_attributes         => p_seg_attributes,
                                p_tablespace             => p_tablespace,
                                p_partname               => p_partname
                              );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END build_constraints;

   -- disables constraints related to a particular table
   -- P_OWNER and P_TABLE are required for this procedure
   PROCEDURE disable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   )
   IS
      l_con_cnt    NUMBER         := 0;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot         := evolve_ot( p_module => 'disable_constraints' );
   BEGIN
      td_ddl.constraint_maint( p_owner                  => p_owner,
                               p_table                  => p_table,
                               p_maint_type             => 'disable',
                               p_constraint_type        => p_constraint_type,
                               p_constraint_regexp      => p_constraint_regexp,
                               p_basis                  => p_basis
                             );
      COMMIT;
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END disable_constraints;

   -- enables constraints related to a particular table
   -- P_OWNER and P_TABLE are required for this procedure
   PROCEDURE enable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   )
   IS
      l_con_cnt    NUMBER         := 0;
      l_tab_name   VARCHAR2( 61 ) := UPPER( p_owner || '.' || p_table );
      o_ev         evolve_ot         := evolve_ot( p_module => 'enable_constraints' );
   BEGIN
      td_ddl.constraint_maint( p_owner                  => p_owner,
                               p_table                  => p_table,
                               p_maint_type             => 'enable',
                               p_constraint_type        => p_constraint_type,
                               p_constraint_regexp      => p_constraint_regexp,
                               p_basis                  => p_basis
                             );
      COMMIT;
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END enable_constraints;

   -- drop particular indexes from a table
   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL
   )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'drop_indexes' );
   BEGIN
      td_ddl.drop_indexes( p_owner             => p_owner,
                           p_table             => p_table,
                           p_index_type        => p_index_type,
                           p_index_regexp      => p_index_regexp
                         );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END drop_indexes;

   -- drop particular constraints from a table
   PROCEDURE drop_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL
   )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'drop_constraints' );
   BEGIN
      td_ddl.drop_constraints( p_owner                  => p_owner,
                               p_table                  => p_table,
                               p_constraint_type        => p_constraint_type,
                               p_constraint_regexp      => p_constraint_regexp
                             );
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
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
      o_ev   evolve_ot := evolve_ot( p_module => 'object_grants' );
   BEGIN
      td_ddl.object_grants( p_owner              => p_owner,
                            p_object             => p_object,
                            p_source_owner       => p_source_owner,
                            p_source_object      => p_source_object,
                            p_grant_regexp       => p_grant_regexp
                          );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
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
      o_ev   evolve_ot := evolve_ot( p_module => 'insert_table' );
   BEGIN
      td_ddl.insert_table( p_owner              => p_owner,
                           p_table              => p_table,
                           p_source_owner       => p_source_owner,
                           p_source_object      => p_source_object,
                           p_trunc              => p_trunc,
                           p_direct             => p_direct,
                           p_degree             => p_degree,
                           p_log_table          => p_log_table,
                           p_reject_limit       => p_reject_limit
                         );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
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
      o_ev   evolve_ot := evolve_ot( p_module => 'merge_table' );
   BEGIN
      td_ddl.merge_table( p_owner              => p_owner,
                          p_table              => p_table,
                          p_source_owner       => p_source_owner,
                          p_source_object      => p_source_object,
                          p_columns            => p_columns,
                          p_direct             => p_direct,
                          p_degree             => p_degree,
                          p_log_table          => p_log_table,
                          p_reject_limit       => p_reject_limit
                        );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
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
      td_ddl.load_tables( p_owner              => p_owner,
                          p_source_owner       => p_source_owner,
                          p_source_regexp      => p_source_regexp,
                          p_suffix             => p_suffix,
                          p_merge              => p_merge,
                          p_part_tabs          => p_part_tabs,
                          p_trunc              => p_trunc,
                          p_direct             => p_direct,
                          p_degree             => p_degree,
                          p_commit             => p_commit
                        );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
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
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_statistics     VARCHAR2 DEFAULT 'transfer',
      p_statpercent    NUMBER DEFAULT NULL,
      p_statdegree     NUMBER DEFAULT NULL,
      p_statmethod     VARCHAR2 DEFAULT NULL
   )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'exchange_partition' );
   BEGIN
      td_ddl.exchange_partition( p_owner             => p_owner,
                                 p_table             => p_table,
                                 p_source_owner      => p_source_owner,
                                 p_source_table      => p_source_table,
                                 p_partname          => p_partname,
                                 p_index_space       => p_index_space,
                                 p_index_drop        => p_index_drop,
                                 p_statistics        => p_statistics,
                                 p_statpercent       => p_statpercent,
                                 p_statdegree        => p_statdegree,
                                 p_statmethod        => p_statmethod
                               );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END exchange_partition;

   PROCEDURE replace_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'replace_table' );
   BEGIN
      td_ddl.replace_table( p_owner             => p_owner,
                            p_table             => p_table,
                            p_source_table      => p_source_table,
                            p_tablespace        => p_tablespace,
                            p_index_drop        => p_index_drop,
                            p_statistics        => p_statistics
                          );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END replace_table;

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
      o_ev   evolve_ot := evolve_ot( p_module => 'unusable_indexes' );
   BEGIN
      td_ddl.unusable_indexes( p_owner              => p_owner,
                               p_table              => p_table,
                               p_partname           => p_partname,
                               p_source_owner       => p_source_owner,
                               p_source_object      => p_source_object,
                               p_source_column      => p_source_column,
                               p_d_num              => p_d_num,
                               p_p_num              => p_p_num,
                               p_index_regexp       => p_index_regexp,
                               p_index_type         => p_index_type,
                               p_part_type          => p_part_type
                             );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END unusable_indexes;

   PROCEDURE usable_indexes( p_owner VARCHAR2, p_table VARCHAR2 )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'usable_indexes' );
   BEGIN
      td_ddl.usable_indexes( p_owner => p_owner, p_table => p_table );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
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
      o_ev   evolve_ot := evolve_ot( p_module => 'update_stats' );
   BEGIN
      td_ddl.update_stats( p_owner                => p_owner,
                           p_table                => p_table,
                           p_partname             => p_partname,
                           p_source_owner         => p_source_owner,
                           p_source_table         => p_source_table,
                           p_source_partname      => p_source_partname,
                           p_percent              => p_percent,
                           p_degree               => p_degree,
                           p_method               => p_method,
                           p_granularity          => p_granularity,
                           p_cascade              => p_cascade,
                           p_options              => p_options
                         );
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END update_stats;
END td_etl;
/