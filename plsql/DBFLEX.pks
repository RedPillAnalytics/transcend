CREATE OR REPLACE PACKAGE tdinc.dbflex AUTHID CURRENT_USER
AS
   PROCEDURE trunc_tab (p_owner IN VARCHAR2, p_table IN VARCHAR2, p_debug BOOLEAN DEFAULT FALSE);

   PROCEDURE build_indexes (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_global         BOOLEAN DEFAULT TRUE,
      p_debug          BOOLEAN DEFAULT FALSE);

   PROCEDURE build_constraints (
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_seg_attributes      BOOLEAN DEFAULT FALSE,
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_debug               BOOLEAN DEFAULT FALSE);

   PROCEDURE drop_indexes (
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_debug          BOOLEAN DEFAULT FALSE);

   PROCEDURE drop_constraints (
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_debug               BOOLEAN DEFAULT FALSE);

   PROCEDURE regexp_load (
      p_source_owner   VARCHAR2,
      p_regexp         VARCHAR2,
      p_owner          VARCHAR2 DEFAULT NULL,
      p_suf_re_rep     VARCHAR2 DEFAULT '?',
      p_merge          BOOLEAN DEFAULT FALSE,
      p_part_tabs      BOOLEAN DEFAULT TRUE,
      p_trunc          BOOLEAN DEFAULT FALSE,
      p_direct         BOOLEAN DEFAULT TRUE,
      p_commit         BOOLEAN DEFAULT TRUE,
      p_debug          BOOLEAN DEFAULT FALSE);

   PROCEDURE table_exchange (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_drop     BOOLEAN DEFAULT TRUE,
      p_stats          VARCHAR2 DEFAULT 'KEEP',
      p_statpercent    NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree     NUMBER DEFAULT DBMS_STATS.default_degree,
      p_statmo         VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_debug          BOOLEAN DEFAULT FALSE);

   PROCEDURE regexp_exchange (
      p_source_owner   VARCHAR2,
      p_regexp         VARCHAR2,
      p_owner          VARCHAR2 DEFAULT NULL,
      p_suf_re_rep     VARCHAR2 DEFAULT '?',
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_drop     BOOLEAN DEFAULT TRUE,
      p_stats          VARCHAR2 DEFAULT 'KEEP',
      p_statpercent    NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree     NUMBER DEFAULT DBMS_STATS.default_degree,
      p_statmo         VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_debug          BOOLEAN DEFAULT FALSE);

   PROCEDURE unusable_indexes (
      p_owner        VARCHAR2,
      p_table        VARCHAR2,
      p_part_name    VARCHAR2 DEFAULT NULL,
      p_index_type   VARCHAR2 DEFAULT NULL,
      p_global       BOOLEAN DEFAULT FALSE,
      p_debug        BOOLEAN DEFAULT FALSE);

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
      p_debug           BOOLEAN DEFAULT FALSE);

   PROCEDURE usable_indexes (p_owner VARCHAR2, p_table VARCHAR2, p_debug BOOLEAN DEFAULT FALSE);
END dbflex;
/