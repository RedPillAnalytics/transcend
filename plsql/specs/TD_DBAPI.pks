CREATE OR REPLACE PACKAGE td_dbapi AUTHID CURRENT_USER
IS
   PROCEDURE trunc_tab(
      p_owner     IN   VARCHAR2,
      p_table     IN   VARCHAR2,
      p_runmode        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE build_indexes(
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE build_constraints(
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_seg_attributes      VARCHAR2 DEFAULT 'no',
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL
   );

   PROCEDURE disable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL
   );

   PROCEDURE enable_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL
   );

   PROCEDURE drop_indexes(
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE drop_constraints(
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL
   );

   PROCEDURE insert_table(
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE merge_table(
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_columns         VARCHAR2 DEFAULT NULL,
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE load_tables(
      p_source_owner    VARCHAR2,
      p_source_regexp   VARCHAR2,
      p_owner           VARCHAR2 DEFAULT NULL,
      p_suffix          VARCHAR2 DEFAULT NULL,
      p_merge           VARCHAR2 DEFAULT 'no',
      p_part_tabs       VARCHAR2 DEFAULT 'yes',
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_degree          NUMBER DEFAULT NULL,
      p_commit          VARCHAR2 DEFAULT 'yes',
      p_runmode         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE exchange_partition(
      p_source_owner     VARCHAR2,
      p_source_table     VARCHAR2,
      p_owner            VARCHAR2,
      p_table            VARCHAR2,
      p_partname         VARCHAR2 DEFAULT NULL,
      p_idx_tablespace   VARCHAR2 DEFAULT NULL,
      p_index_drop       VARCHAR2 DEFAULT 'yes',
      p_handle_fkeys     VARCHAR2 DEFAULT 'yes',
      p_statistics       VARCHAR2 DEFAULT NULL,
      p_statpercent      NUMBER DEFAULT NULL,
      p_statdegree       NUMBER DEFAULT NULL,
      p_statmethod       VARCHAR2 DEFAULT NULL,
      p_runmode          VARCHAR2 DEFAULT NULL
   );

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
      p_part_type       VARCHAR2 DEFAULT NULL,
      p_runmode         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE usable_indexes(
      p_owner     VARCHAR2,
      p_table     VARCHAR2,
      p_runmode   VARCHAR2 DEFAULT NULL
   );

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
      p_cascade           BOOLEAN DEFAULT NULL,
      p_options           VARCHAR2 DEFAULT 'GATHER AUTO',
      p_runmode           VARCHAR2 DEFAULT NULL
   );
END td_dbapi;
/