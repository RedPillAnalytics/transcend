CREATE OR REPLACE PACKAGE tdinc.owb_utils
AS
   PROCEDURE start_map_control(
      p_jobname        VARCHAR2,
      p_trg_owner      VARCHAR2 DEFAULT NULL,
      p_trg_table      VARCHAR2 DEFAULT NULL,
      p_part_name      VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_src_owner      VARCHAR2 DEFAULT NULL,
      p_src_obj        VARCHAR2 DEFAULT NULL,
      p_src_col        VARCHAR2 DEFAULT NULL,
      p_global         BOOLEAN DEFAULT FALSE,
      p_src_col_dnum   NUMBER DEFAULT NULL,
      p_src_col_pnum   NUMBER DEFAULT NULL );

   PROCEDURE end_map_control(
      p_trg_owner   VARCHAR2 DEFAULT NULL,
      p_trg_table   VARCHAR2 DEFAULT NULL );

   PROCEDURE run_process_flow(
      p_flow_name       VARCHAR2,
      p_flow_location   VARCHAR2 DEFAULT 'OWF_LOCATION' );
END owb_utils;
/