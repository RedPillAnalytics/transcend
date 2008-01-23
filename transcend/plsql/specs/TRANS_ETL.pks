CREATE OR REPLACE PACKAGE trans_etl AUTHID CURRENT_USER
IS
   PROCEDURE start_etl_mapping(
      p_mapping         VARCHAR2 DEFAULT $$plsql_unit,
      p_owner           VARCHAR2 DEFAULT NULL,
      p_table           VARCHAR2 DEFAULT NULL,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT NULL,
      p_p_num           NUMBER DEFAULT NULL,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL,
      p_batch_id        NUMBER DEFAULT NULL
   );

   PROCEDURE end_etl_mapping(
      p_mapping        VARCHAR2 DEFAULT $$plsql_unit,
      p_owner          VARCHAR2 DEFAULT NULL,
      p_table          VARCHAR2 DEFAULT NULL,
      p_source_owner   VARCHAR2 DEFAULT NULL,
      p_source_table   VARCHAR2 DEFAULT NULL,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT NULL,
      p_statistics     VARCHAR2 DEFAULT NULL
   );

   PROCEDURE truncate_table( p_owner VARCHAR2, p_table VARCHAR2, p_reuse VARCHAR2 DEFAULT 'no' );

   PROCEDURE drop_table( p_owner VARCHAR2, p_table VARCHAR2, p_purge VARCHAR2 DEFAULT 'yes' );

   PROCEDURE build_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_partitioning   VARCHAR2 DEFAULT 'yes',
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
      p_concurrent     VARCHAR2 DEFAULT 'no'
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
      p_concurrent          VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE disable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table'
   );

   PROCEDURE enable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_basis               VARCHAR2 DEFAULT 'table',
      p_concurrent          VARCHAR2 DEFAULT 'no'
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
   );

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
   );

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
   );

   PROCEDURE exchange_partition(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   );

   PROCEDURE load_dim( p_owner VARCHAR2, p_table VARCHAR2 );

   PROCEDURE replace_table(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_source_table   VARCHAR2,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_concurrent     VARCHAR2 DEFAULT 'no',
      p_statistics     VARCHAR2 DEFAULT 'transfer'
   );

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
   );

   PROCEDURE usable_indexes( p_owner VARCHAR2, p_table VARCHAR2, p_concurrent VARCHAR2 DEFAULT 'no' );

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
   );
END trans_etl;
/