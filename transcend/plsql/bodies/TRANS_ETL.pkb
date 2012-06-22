CREATE OR REPLACE PACKAGE BODY trans_etl
AS
   PROCEDURE start_mapping(
      p_mapping    VARCHAR2 DEFAULT SYS_CONTEXT( 'USERENV', 'ACTION' ),
      p_batch_id   NUMBER DEFAULT NULL
   )
   AS
      o_ev    evolve_ot  := evolve_ot( p_module => 'start_mapping' );

      o_map   mapping_ot := trans_factory.get_mapping_ot( p_mapping  => p_mapping, 
                                                          p_batch_id => p_batch_id );
   BEGIN
      evolve.log_variable( 'MAPPING_TYPE', o_map.mapping_type);
      -- now, regardless of which object type this is, the following call is correct
      o_map.start_map;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END start_mapping;

   PROCEDURE end_mapping(
      p_mapping    VARCHAR2 DEFAULT SYS_CONTEXT( 'USERENV', 'ACTION' ),
      p_batch_id   NUMBER DEFAULT NULL,
      p_results    NUMBER DEFAULT NULL
   )
   AS
      o_ev    evolve_ot      := evolve_ot( p_module => 'start_mapping' );
      o_map   mapping_ot := trans_factory.get_mapping_ot( p_mapping  => p_mapping, 
                                                          p_batch_id => p_batch_id );
   BEGIN
      
      evolve.log_variable( 'o_map.mapping_type',o_map.mapping_type );

      -- if the P_RESULTS parameter is populated, then write a count record
      IF p_results IS NOT NULL
      THEN 

      evolve.log_results_msg( p_count      => p_results,
                              p_owner      => o_map.table_owner,
                              p_object     => o_map.table_name,
                              p_category   => 'mapping'
                            );
         
      END IF;

      -- now, regardless of which object type this is, the following call is correct
      o_map.end_map;
   -- used to have a commit here.
   -- I don't think a commit should be done inside a mapping
   -- it overrides the commit control of an ETL tool (if any)
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END end_mapping;

   PROCEDURE truncate_table( p_owner VARCHAR2, p_table VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' )
   IS
   BEGIN
      td_dbutils.truncate_table( p_owner => p_owner, p_table => p_table, p_reuse => p_reuse );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END truncate_table;
   
   PROCEDURE truncate_partition
      ( p_owner VARCHAR2,
        p_table VARCHAR2, 
        p_partname VARCHAR2 
      )
   IS
   BEGIN

      td_dbutils.partition_action
      ( p_owner         => p_owner, 
        p_table         => p_table, 
        p_partname      => p_partname,
        p_action        => 'truncate' );
      
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END truncate_partition;

   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2 DEFAULT 'yes' )
   IS
   BEGIN
      td_dbutils.drop_table( p_owner => p_owner, p_table => p_table, p_purge => p_purge );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END drop_table;
   
   PROCEDURE drop_partition
      ( p_owner VARCHAR2,
        p_table VARCHAR2, 
        p_partname VARCHAR2 
      )
   IS
   BEGIN

      td_dbutils.partition_action
      ( p_owner         => p_owner, 
        p_table         => p_table, 
        p_partname      => p_partname,
        p_action        => 'drop' );
      
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END drop_partition;

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
   BEGIN
      td_dbutils.build_table( p_owner             => p_owner,
                              p_table             => p_table,
                              p_source_owner      => p_source_owner,
                              p_source_table      => p_source_table,
                              p_tablespace        => p_tablespace,
                              p_partitioning      => p_partitioning,
                              p_rows              => p_rows,
                              p_statistics        => p_statistics,
			      p_indexes		  => p_indexes,
			      p_constraints	  => p_constraints,
                              p_grants            => p_grants
                            );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
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
         evolve.log_err;
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
         evolve.log_err;
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
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
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
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END enable_constraints;

   -- validates constraints related to a particular table
   -- P_OWNER and P_TABLE are required for this procedure
   PROCEDURE validate_constraints(
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
                                   p_maint_type             => 'validate',
                                   p_constraint_type        => p_constraint_type,
                                   p_constraint_regexp      => p_constraint_regexp,
                                   p_basis                  => p_basis,
                                   p_concurrent             => p_concurrent
                                 );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END validate_constraints;
   
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
         evolve.log_err;
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
      WHEN td_dbutils.drop_iot_key
      THEN
         NULL;
      WHEN OTHERS
      THEN
         evolve.log_err;
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
         evolve.log_err;
         RAISE;
   END object_grants;

   -- structures an insert or insert append statement from the source to the target provided
   PROCEDURE insert_table
      (
        p_owner           VARCHAR2,
        p_table           VARCHAR2,
        p_source_owner    VARCHAR2,
        p_source_object   VARCHAR2,
        p_dblink          VARCHAR2      DEFAULT NULL,
        p_scn             NUMBER        DEFAULT NULL,
        p_trunc           VARCHAR2      DEFAULT 'no',
        p_direct          VARCHAR2      DEFAULT 'yes',
        p_degree          NUMBER        DEFAULT NULL,
        p_log_table       VARCHAR2      DEFAULT NULL,
        p_reject_limit    VARCHAR2      DEFAULT 'unlimited'
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
                               p_reject_limit       => p_reject_limit,
                               p_scn                => p_scn,
                               p_dblink             => p_dblink
                             );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
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
         evolve.log_err;
         RAISE;
   END merge_table;

   -- queries the dictionary based on regular expressions and loads tables using either the load_tab method or the merge_tab method
   PROCEDURE load_tables
      (
        p_owner           VARCHAR2,
        p_source_owner    VARCHAR2,
        p_source_regexp   VARCHAR2 DEFAULT NULL,
        p_source_type     VARCHAR2 DEFAULT 'table',
        p_suffix          VARCHAR2 DEFAULT NULL,
        p_dblink          VARCHAR2 DEFAULT NULL,
        p_scn             VARCHAR2 DEFAULT NULL,
        p_merge           VARCHAR2 DEFAULT 'no',
        p_trunc           VARCHAR2 DEFAULT 'no',
        p_direct          VARCHAR2 DEFAULT 'yes',
        p_degree          NUMBER   DEFAULT NULL,
        p_commit          VARCHAR2 DEFAULT 'yes',
        p_raise_err       VARCHAR2 DEFAULT 'yes'
      )
   IS
      l_rows   BOOLEAN := FALSE;
   BEGIN
      td_dbutils.load_tables( p_owner              => p_owner,
                              p_source_owner       => p_source_owner,
                              p_source_regexp      => p_source_regexp,
                              p_suffix             => p_suffix,
                              p_merge              => p_merge,
                              p_trunc              => p_trunc,
                              p_direct             => p_direct,
                              p_degree             => p_degree,
                              p_commit             => p_commit,
                              p_scn                => p_scn,
                              p_dblink             => p_dblink,
                              p_raise_err          => p_raise_err
                            );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END load_tables;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_partition(
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_source_owner       VARCHAR2,
      p_source_table       VARCHAR2,
      p_partname           VARCHAR2 DEFAULT NULL,
      p_index_space        VARCHAR2 DEFAULT NULL,
      p_idx_concurrency    VARCHAR2 DEFAULT 'no',
      p_con_concurrency    VARCHAR2 DEFAULT 'no',
      p_drop_deps          VARCHAR2 DEFAULT 'yes',
      p_statistics         VARCHAR2 DEFAULT 'transfer'
   )
   IS
   BEGIN
      td_dbutils.exchange_partition( p_owner             => p_owner,
                                     p_table             => p_table,
                                     p_source_owner      => p_source_owner,
                                     p_source_table      => p_source_table,
                                     p_partname          => p_partname,
                                     p_index_space       => p_index_space,
                                     p_idx_concurrency   => p_idx_concurrency,
                                     p_con_concurrency   => p_con_concurrency,
                                     p_drop_deps         => p_drop_deps,
                                     p_statistics        => p_statistics
                                   );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END exchange_partition;

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
   BEGIN
      td_dbutils.replace_table( p_owner             => p_owner,
                                p_table             => p_table,
                                p_source_table      => p_source_table,
                                p_tablespace        => p_tablespace,
                                p_idx_concurrency   => p_idx_concurrency,
                                p_con_concurrency   => p_con_concurrency,
                                p_statistics        => p_statistics
                              );
      -- clear out temporary table holding index and constraint statements
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END replace_table;

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
   BEGIN
      td_dbutils.usable_indexes( p_owner        => p_owner, 
                                 p_table        => p_table,
                                 p_partname     => p_partname,
                                 p_index_regexp => p_index_regexp,
                                 p_index_type   => p_index_type,
                                 p_part_type    => p_part_type,
                                 p_concurrent   => p_concurrent );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
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
      o_ev          evolve_ot      := evolve_ot( p_module => 'transfer_stats' );
   BEGIN

      td_dbutils.transfer_stats( p_owner              => p_owner,
                                 p_segment            => p_segment,
                                 p_source_owner       => p_source_owner,
                                 p_source_segment     => p_source_segment,
                                 p_partname           => p_partname,
                                 p_source_partname    => p_source_partname,
                                 p_segment_type       => p_segment_type
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
      o_ev          evolve_ot      := evolve_ot( p_module => 'gather_stats' );
   BEGIN

      td_dbutils.gather_stats( p_owner              => p_owner,
                               p_segment            => p_segment,
                               p_partname           => p_partname,
                               p_percent            => p_percent,
                               p_degree             => p_degree,
                               p_method             => p_method,
                               p_granularity        => p_granularity,
                               p_cascade            => p_cascade,
                               p_segment_type       => p_segment_type
                             );
      
           
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END gather_stats;
   
   PROCEDURE add_range_part( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_value           VARCHAR2,
      p_tablespace      VARCHAR2 DEFAULT NULL,
      p_compress        VARCHAR2 DEFAULT 'no'
   )
   IS
      o_ev          evolve_ot      := evolve_ot( p_module => 'add_range_part' );
   BEGIN

      td_dbutils.add_range_part( p_owner        => p_owner,
                                 p_table        => p_table,
                                 p_partname     => p_partname,
                                 p_value        => p_value,
                                 p_tablespace   => p_tablespace,
                                 p_compress     => p_compress );
      
           
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END add_range_part;

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
      o_ev          evolve_ot      := evolve_ot( p_module => 'add_range_list_subpart' );
   BEGIN

      td_dbutils.add_range_list_subpart( p_owner        => p_owner,
                                         p_table        => p_table,
                                         p_partname     => p_partname,
                                         p_subpartname  => p_subpartname,
                                         p_value        => p_value,
                                         p_tablespace   => p_tablespace,
                                         p_compress     => p_compress );
      
           
      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END add_range_list_subpart;

   FUNCTION execute_mapping_bool( 
      p_mapping    VARCHAR2
   )
      RETURN BOOLEAN
   AS
      l_status       mapping_control.status%type;

   BEGIN

      BEGIN
                  
      -- look for a completed record from the mapping_status table
         SELECT status
           INTO l_status
           FROM mapping_control
          WHERE lower(mapping_name) = lower( p_mapping )
            AND status = 'complete';
      EXCEPTION
         
         -- if no record is found, then we are OK to run
         WHEN no_data_found
         THEN 
            RETURN TRUE;
      END;
            
      -- otherwise, we need to return a FALSE because this has already completed successfully 
      RETURN FALSE;

   END execute_mapping_bool;

   FUNCTION execute_mapping_num( 
      p_mapping    VARCHAR2
   )
      RETURN NUMBER
   AS
      l_results BOOLEAN;
   BEGIN
      
      l_results := execute_mapping_bool( p_mapping );
      
      IF l_results
      THEN 
         RETURN 0;
      ELSE
         RETURN 1;
      END IF;

   END execute_mapping_num;

   FUNCTION execute_mapping_str( 
      p_mapping    VARCHAR2
   )
      RETURN VARCHAR2
   AS
      l_results BOOLEAN;
   BEGIN
      
      l_results := execute_mapping_bool( p_mapping );
      
      IF l_results
      THEN 
         RETURN 'Y';
      ELSE
         RETURN 'N';
      END IF;

   END execute_mapping_str;
   
   PROCEDURE reset_map_control
   AS
      o_ev    evolve_ot  := evolve_ot( p_module => 'reset_map_control' );

   BEGIN
      
      -- update all statuses to ready
      UPDATE mapping_control
         SET status='ready';

   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END reset_map_control;

   FUNCTION get_scn
      RETURN NUMBER 
   AS
   BEGIN
      
      RETURN evolve.get_scn;

   END get_scn;
   
END trans_etl;
/

SHOW errors