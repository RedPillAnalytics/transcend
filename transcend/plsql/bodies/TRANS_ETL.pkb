CREATE OR REPLACE PACKAGE BODY trans_etl
AS
   PROCEDURE start_etl_mapping(
      p_mapping    VARCHAR2 DEFAULT SYS_CONTEXT( 'USERENV', 'ACTION' ),
      p_batch_id   NUMBER DEFAULT NULL
   )
   AS
      o_map   mapping_ot := mapping_ot( p_mapping => p_mapping, p_batch_id => p_batch_id );
      o_dim   dimension_ot := dimension_ot( p_mapping => p_mapping, p_batch_id => p_batch_id );
      l_map_type  mapping_conf.mapping_type%type;
   BEGIN
      -- first, find out what kind of mapping we have
      -- there are currently two types supported... table and dimension
      SELECT mapping_type
	INTO l_map_type
	FROM mapping_conf
       WHERE mapping_name = p_mapping;
      
      -- polymorph the type based on the results
      IF lower( l_map_type ) = 'dimension'
      THEN
	 o_map := o_dim;
      END IF;

      -- now, start the mapping      
      o_map.start_map;

   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END start_etl_mapping;

   PROCEDURE end_etl_mapping( p_mapping VARCHAR2 DEFAULT SYS_CONTEXT( 'USERENV', 'ACTION' ))
   AS
      o_map   mapping_ot := mapping_ot( p_mapping => p_mapping );
   BEGIN
      o_map.end_map;
      -- used to have a commit here.
      -- I don't think a commit should be done inside a mapping
      -- it overrides the commit control of an ETL tool (if any)
   END end_etl_mapping;

   PROCEDURE truncate_table( p_owner VARCHAR2, p_table VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' )
   IS
   BEGIN
      td_dbutils.truncate_table( p_owner => p_owner, p_table => p_table, p_reuse => p_reuse );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END truncate_table;

   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2 DEFAULT 'yes' )
   IS
   BEGIN
      td_dbutils.drop_table( p_owner => p_owner, p_table => p_table, p_purge => p_purge );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.build_table( p_owner             => p_owner,
                              p_table             => p_table,
                              p_source_owner      => p_source_owner,
                              p_source_table      => p_source_table,
                              p_tablespace        => p_tablespace,
                              p_partitioning      => p_partitioning,
                              p_rows              => p_rows,
                              p_statistics        => p_statistics
                            );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
      p_partname       VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no'
   )
   IS
   BEGIN
      td_dbutils.build_indexes( p_owner             => p_owner,
                                p_table             => p_table,
                                p_source_owner      => p_source_owner,
                                p_source_table      => p_source_table,
                                p_index_regexp      => p_index_regexp,
                                p_index_type        => p_index_type,
                                p_part_type         => p_part_type,
                                p_tablespace        => p_tablespace,
                                p_partname          => p_partname,
                                p_concurrent        => p_concurrent
                              );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END build_indexes;

   -- renames cloned indexes on a particular table back to their original names
   PROCEDURE rename_indexes
   IS
      l_idx_cnt   NUMBER    := 0;
      l_rows      BOOLEAN   := FALSE;
      o_ev        evolve_ot := evolve_ot( p_module => 'rename_indexes' );
   BEGIN
      FOR c_idxs IN ( SELECT *
                       FROM td_build_idx_gtt )
      LOOP
         BEGIN
            l_rows       := TRUE;
            evolve_app.exec_sql( p_sql => c_idxs.rename_ddl, p_auto => 'yes' );
            evolve_log.log_msg( c_idxs.rename_msg, 3 );
            l_idx_cnt    := l_idx_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         evolve_log.log_msg( 'No previously cloned indexes identified' );
      ELSE
         evolve_log.log_msg( l_idx_cnt || ' index' || CASE
                                WHEN l_idx_cnt = 1
                                   THEN NULL
                                ELSE 'es'
                             END || ' renamed' );
      END IF;

      -- commit is required to clear out the contents of the global temporary table
      COMMIT;
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         o_ev.clear_app_info;
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
      p_basis               VARCHAR2 DEFAULT 'table',
      p_concurrent          VARCHAR2 DEFAULT 'no'
   )
   IS
   BEGIN
      td_dbutils.build_constraints( p_owner                  => p_owner,
                                    p_table                  => p_table,
                                    p_source_owner           => p_source_owner,
                                    p_source_table           => p_source_table,
                                    p_constraint_type        => p_constraint_type,
                                    p_constraint_regexp      => p_constraint_regexp,
                                    p_basis                  => p_basis,
                                    p_concurrent             => p_concurrent
                                  );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.constraint_maint( p_owner                  => p_owner,
                                   p_table                  => p_table,
                                   p_maint_type             => 'disable',
                                   p_constraint_type        => p_constraint_type,
                                   p_constraint_regexp      => p_constraint_regexp,
                                   p_basis                  => p_basis
                                 );
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END disable_constraints;

   -- enables constraints related to a particular table
   -- P_OWNER and P_TABLE are required for this procedure
   PROCEDURE enable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table',
      p_concurrent          VARCHAR2 DEFAULT 'no'
   )
   IS
   BEGIN
      td_dbutils.constraint_maint( p_owner                  => p_owner,
                                   p_table                  => p_table,
                                   p_maint_type             => 'enable',
                                   p_constraint_type        => p_constraint_type,
                                   p_constraint_regexp      => p_constraint_regexp,
                                   p_basis                  => p_basis,
                                   p_concurrent             => p_concurrent
                                 );
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END enable_constraints;

   -- drop particular indexes from a table
   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      td_dbutils.drop_indexes( p_owner             => p_owner,
                               p_table             => p_table,
                               p_index_type        => p_index_type,
                               p_index_regexp      => p_index_regexp,
                               p_part_type         => p_part_type
                             );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.drop_constraints( p_owner                  => p_owner,
                                   p_table                  => p_table,
                                   p_constraint_type        => p_constraint_type,
                                   p_constraint_regexp      => p_constraint_regexp,
                                   p_basis                  => p_basis
                                 );
   EXCEPTION
      WHEN td_dbutils.e_drop_iot_key
      THEN
         NULL;
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.object_grants( p_owner              => p_owner,
                                p_object             => p_object,
                                p_source_owner       => p_source_owner,
                                p_source_object      => p_source_object,
                                p_grant_regexp       => p_grant_regexp
                              );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.insert_table( p_owner              => p_owner,
                               p_table              => p_table,
                               p_source_owner       => p_source_owner,
                               p_source_object      => p_source_object,
                               p_trunc              => p_trunc,
                               p_direct             => p_direct,
                               p_degree             => p_degree,
                               p_log_table          => p_log_table,
                               p_reject_limit       => p_reject_limit
                             );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.merge_table( p_owner              => p_owner,
                              p_table              => p_table,
                              p_source_owner       => p_source_owner,
                              p_source_object      => p_source_object,
                              p_columns            => p_columns,
                              p_direct             => p_direct,
                              p_degree             => p_degree,
                              p_log_table          => p_log_table,
                              p_reject_limit       => p_reject_limit
                            );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.load_tables( p_owner              => p_owner,
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
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END load_tables;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_partition(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   )
   IS
   BEGIN
      td_dbutils.exchange_partition( p_owner             => p_owner,
                                     p_table             => p_table,
                                     p_source_owner      => p_source_owner,
                                     p_source_table      => p_source_table,
                                     p_index_space       => p_index_space,
                                     p_concurrent        => p_concurrent,
                                     p_statistics        => p_statistics
                                   );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END exchange_partition;

   PROCEDURE replace_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   )
   IS
   BEGIN
      td_dbutils.replace_table( p_owner             => p_owner,
                                p_table             => p_table,
                                p_source_table      => p_source_table,
                                p_tablespace        => p_tablespace,
                                p_concurrent        => p_concurrent,
                                p_statistics        => p_statistics
                              );
      -- clear out temporary table holding index and constraint statements
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END replace_table;

   -- uses SQL analytics to load a hybrid SCD dimension table
   PROCEDURE load_dim( p_owner VARCHAR2, p_table VARCHAR2 )
   IS
      o_dim   dimension_ot;
      l_mapping mapping_conf.mapping_name%type;
   BEGIN
      -- get the mapping name using owner and table
      BEGIN
	 SELECT mapping_name
	   INTO l_mapping
	   FROM mapping_conf JOIN dimension_conf
		USING (table_owner,table_name)
	  WHERE table_owner=p_owner AND table_name=p_table;
      EXCEPTION
	 WHEN no_data_found
	 THEN
           evolve_log.raise_err( 'no_dim', p_owner||'.'||p_table );
      END;
      -- instantiate the dimension object
      o_dim := dimension_ot( p_mapping => l_mapping );
      -- execute the load
      o_dim.LOAD;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END load_dim;

   PROCEDURE unusable_indexes(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      td_dbutils.unusable_indexes( p_owner              => p_owner,
                                   p_table              => p_table,
                                   p_partname           => p_partname,
                                   p_source_owner       => p_source_owner,
                                   p_source_object      => p_source_object,
                                   p_source_column      => p_source_column,
                                   p_index_regexp       => p_index_regexp,
                                   p_index_type         => p_index_type,
                                   p_part_type          => p_part_type
                                 );
      COMMIT;
   END unusable_indexes;

   PROCEDURE usable_indexes( p_owner VARCHAR2, p_table VARCHAR2, p_concurrent VARCHAR2 DEFAULT 'no' )
   IS
   BEGIN
      td_dbutils.usable_indexes( p_owner => p_owner, p_table => p_table, p_concurrent => p_concurrent );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
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
   BEGIN
      td_dbutils.update_stats( p_owner                => p_owner,
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
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END update_stats;
END trans_etl;
/

SHOW errors