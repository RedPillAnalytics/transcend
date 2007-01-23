CREATE OR REPLACE PACKAGE efw.file_mover
IS
   PROCEDURE register_file (
      p_jobnumber            file_ctl.jobnumber%type DEFAULT NULL,
      p_jobname              file_ctl.jobname%type DEFAULT NULL,
      p_filename             file_ctl.filename%type DEFAULT NULL,
      p_source_regexp        file_ctl.source_regexp%type DEFAULT NULL,
      p_regexp_ci_ind        file_ctl.regexp_ci_ind%type DEFAULT NULL,
      p_source_dir           file_ctl.source_dir%type DEFAULT NULL,
      p_min_bytes            file_ctl.min_bytes%type DEFAULT NULL,
      p_max_bytes            file_ctl.max_bytes%type DEFAULT NULL,
      p_arch_dir             file_ctl.arch_dir%type DEFAULT NULL,
      p_add_arch_ts_ind      file_ctl.add_arch_ts_ind%type DEFAULT NULL,
      p_wrk_dir              file_ctl.wrk_dir%type DEFAULT NULL,
      p_ext_dir              file_ctl.ext_dir%type DEFAULT NULL,
      p_ext_filename         file_ctl.ext_filename%type DEFAULT NULL,
      p_ext_table            file_ctl.ext_table%type DEFAULT NULL,
      p_ext_tab_owner        file_ctl.ext_tab_owner%type DEFAULT NULL,
      p_multi_files_action   file_ctl.multi_files_action%type DEFAULT NULL,
      p_files_required_ind   file_ctl.files_required_ind%type DEFAULT NULL);

   FUNCTION calc_rej_ind (p_jobnumber NUMBER, p_rej_limit NUMBER DEFAULT 20)
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