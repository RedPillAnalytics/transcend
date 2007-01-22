CREATE OR REPLACE PACKAGE common.file_mover
IS
   PROCEDURE register_job_file (
      p_jobnumber       NUMBER DEFAULT NULL,
      p_jobname         VARCHAR2 DEFAULT NULL,
      p_filename        VARCHAR2 DEFAULT NULL,
      p_source_regexp   VARCHAR2 DEFAULT NULL,
      p_regexp_ci_ind   VARCHAR2 DEFAULT NULL,
      p_source_dir      VARCHAR2 DEFAULT NULL,
      p_min_bytes       NUMBER DEFAULT NULL,
      p_max_bytes       NUMBER DEFAULT NULL,
      p_arch_dir        VARCHAR2 DEFAULT NULL,
      p_add_arch_ts     VARCHAR2 DEFAULT NULL,
      p_wrk_dir         VARCHAR2 DEFAULT NULL,
      p_ext_dir         VARCHAR2 DEFAULT NULL,
      p_ext_filename    VARCHAR2 DEFAULT NULL,
      p_ext_table       VARCHAR2 DEFAULT NULL,
      p_ext_tab_owner   VARCHAR2 DEFAULT NULL,
      p_mul_fil_act     VARCHAR2 DEFAULT NULL,
      p_files_req_ind   VARCHAR2 DEFAULT NULL);

   FUNCTION calc_rej_ind (
      p_jobnumber   NUMBER,
      p_rej_limit   NUMBER DEFAULT 20)
      RETURN VARCHAR2;

   PROCEDURE process_job (
      p_jobname       VARCHAR2,
      p_filename      VARCHAR2 DEFAULT NULL,
      p_keep_source   BOOLEAN DEFAULT FALSE,
      p_debug         BOOLEAN DEFAULT FALSE);

   PROCEDURE process_jobnumber (
      p_jobnumber     NUMBER,
      p_keep_source   BOOLEAN DEFAULT FALSE,
      p_debug         BOOLEAN DEFAULT FALSE);
END file_mover;
/