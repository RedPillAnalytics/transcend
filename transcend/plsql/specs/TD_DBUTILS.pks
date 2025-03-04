CREATE OR REPLACE PACKAGE td_dbutils AUTHID CURRENT_USER
IS

   default_tablespace CONSTANT VARCHAR2(30) := '#*default_tablespace*#';

   drop_iot_key   EXCEPTION;

   PROCEDURE populate_partname(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 3,
      p_p_num           NUMBER DEFAULT 1048576
   );

   PROCEDURE truncate_table( p_owner VARCHAR2, p_table VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' );
      
   PROCEDURE truncate_schema( p_schema VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' );

   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2 DEFAULT 'yes' );

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
   );

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
   );

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
   );

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
   );
      
   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL
   );

   PROCEDURE drop_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   );

   PROCEDURE object_grants(
      p_owner           VARCHAR2,
      p_object          VARCHAR2,
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_grant_regexp    VARCHAR2 DEFAULT NULL
   );

   PROCEDURE insert_table
      (
        p_owner           VARCHAR2,
        p_table           VARCHAR2,
        p_source_owner    VARCHAR2,
        p_source_object   VARCHAR2,
        p_scn             NUMBER        DEFAULT NULL,
        p_trunc           VARCHAR2      DEFAULT 'no',
        p_direct          VARCHAR2      DEFAULT 'yes',
        p_degree          NUMBER        DEFAULT NULL,
        p_log_table       VARCHAR2      DEFAULT NULL,
        p_reject_limit    VARCHAR2      DEFAULT 'unlimited',
        p_dblink          VARCHAR2      DEFAULT NULL
      );

   PROCEDURE merge_table
      (
        p_owner           VARCHAR2,
        p_table           VARCHAR2,
        p_source_owner    VARCHAR2,
        p_source_object   VARCHAR2,
        p_columns         VARCHAR2 DEFAULT NULL,
        p_direct          VARCHAR2 DEFAULT 'yes',
        p_degree          NUMBER   DEFAULT NULL,
        p_log_table       VARCHAR2 DEFAULT NULL,
        p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
        p_check_table     VARCHAR2 DEFAULT 'yes'
      );

   PROCEDURE load_tables
      (
        p_owner           VARCHAR2,
        p_source_owner    VARCHAR2,
        p_source_regexp   VARCHAR2 DEFAULT '.',
        p_source_type     VARCHAR2 DEFAULT 'table',
        p_suffix          VARCHAR2 DEFAULT NULL,
        p_scn             VARCHAR2 DEFAULT NULL,
        p_merge           VARCHAR2 DEFAULT 'no',
        p_trunc           VARCHAR2 DEFAULT 'no',
        p_direct          VARCHAR2 DEFAULT 'yes',
        p_degree          NUMBER   DEFAULT NULL,
        p_commit          VARCHAR2 DEFAULT 'yes',
        p_raise_err       VARCHAR2 DEFAULT 'yes'
   );

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
   );

   PROCEDURE replace_table(
      p_owner             VARCHAR2,
      p_table             VARCHAR2,
      p_source_table      VARCHAR2,
      p_tablespace        VARCHAR2 DEFAULT NULL,
      p_idx_concurrency   VARCHAR2 DEFAULT 'no',
      p_con_concurrency   VARCHAR2 DEFAULT 'no',
      p_statistics        VARCHAR2 DEFAULT 'transfer'
   );

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
   );

   PROCEDURE usable_indexes(
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL,
      p_concurrent      VARCHAR2 DEFAULT 'no' 
   );

   PROCEDURE transfer_stats(
      p_owner             VARCHAR2,
      p_segment           VARCHAR2,
      p_source_owner      VARCHAR2,
      p_source_segment    VARCHAR2,
      p_partname          VARCHAR2 DEFAULT NULL,
      p_source_partname   VARCHAR2 DEFAULT NULL,
      p_segment_type      VARCHAR2 DEFAULT NULL
   );

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
   );

   PROCEDURE add_range_part( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_value           VARCHAR2,
      p_tablespace      VARCHAR2 DEFAULT NULL,
      p_compress        VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE add_range_list_subpart( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_subpartname     VARCHAR2,
      p_value           VARCHAR2,
      p_tablespace      VARCHAR2 DEFAULT NULL,
      p_compress        VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE partition_action( 
      p_owner           VARCHAR2, 
      p_table           VARCHAR2,
      p_partname        VARCHAR2,
      p_action          VARCHAR2 DEFAULT 'truncate'
   );

END td_dbutils;
/