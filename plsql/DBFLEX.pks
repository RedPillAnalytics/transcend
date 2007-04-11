CREATE OR REPLACE PACKAGE tdinc.dbflex AUTHID CURRENT_USER
AS
   PROCEDURE trunc_tab (p_owner IN VARCHAR2, p_table IN VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL);

   PROCEDURE build_indexes (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL);

   PROCEDURE build_constraints (
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_seg_attributes      VARCHAR2 DEFAULT 'no',
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL);

   PROCEDURE drop_indexes (
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL);

   PROCEDURE drop_constraints (
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL);

   PROCEDURE insert_table (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT NULL);

   PROCEDURE merge_table (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_columns         VARCHAR2 DEFAULT NULL,
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT 'no');

   PROCEDURE load_tables (
      p_source_owner    VARCHAR2,
      p_source_regexp   VARCHAR2,
      p_owner           VARCHAR2 DEFAULT NULL,
      p_suffix          VARCHAR2 DEFAULT NULL,
      p_merge           VARCHAR2 DEFAULT 'no',
      p_part_tabs       VARCHAR2 DEFAULT 'yes',
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_commit          VARCHAR2 DEFAULT 'yes',
      p_runmode         VARCHAR2 DEFAULT NULL);

   PROCEDURE exchange_partition (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
				  p_partname       VARCHAR2 DEFAULT NULL,
				  p_idx_tablespace VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT 'yes',
      p_gather_stats   VARCHAR2 DEFAULT 'yes',
      p_statpercent    NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree     NUMBER DEFAULT DBMS_STATS.auto_degree,
      p_statmethod     VARCHAR2 DEFAULT DBMS_STATS.get_param ('method_opt'),
      p_runmode        VARCHAR2 DEFAULT NULL);

   PROCEDURE unusable_indexes (
      p_owner        VARCHAR2,
      p_table        VARCHAR2,
      p_part_name    VARCHAR2 DEFAULT NULL,
      p_index_type   VARCHAR2 DEFAULT NULL,
      p_global       BOOLEAN DEFAULT FALSE,
      p_runmode      VARCHAR2 DEFAULT NULL);

   PROCEDURE unusable_idx_src (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_global          BOOLEAN DEFAULT FALSE,
      p_d_num           NUMBER DEFAULT 0,
      p_p_num           NUMBER DEFAULT 65535,
      p_runmode         VARCHAR2 DEFAULT NULL);

   PROCEDURE usable_indexes (p_owner VARCHAR2, p_table VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL);
END dbflex;
/