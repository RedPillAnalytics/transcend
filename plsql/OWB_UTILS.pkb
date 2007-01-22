CREATE OR REPLACE PACKAGE BODY common.owb_utils
AS
   g_app   app_info;

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
      p_src_col_pnum   NUMBER DEFAULT NULL )
   AS
   BEGIN
      g_app :=
         app_info( p_client_info =>      p_jobname,
                   p_action =>           'OWB mapping',
                   p_module =>           utility.get_package_name );
      job.log_msg( 'Begin mapping' );

      CASE
         -- if we have target and source information, that means we want to evaluate a source table
         -- to see which indexes to mark unusable on a target table.
      WHEN     p_trg_owner IS NOT NULL
           AND p_trg_table IS NOT NULL
           AND p_src_owner IS NOT NULL
           AND p_src_obj IS NOT NULL
           AND p_src_col IS NOT NULL
         THEN
            etl.unusable_idx_src( p_owner =>          p_trg_owner,
                                  p_table =>          p_trg_table,
                                  p_src_owner =>      p_src_owner,
                                  p_src_obj =>        p_src_obj,
                                  p_src_col =>        p_src_col,
                                  p_d_num =>          nvl(p_src_col_dnum,0),
				  p_p_num =>          nvl(p_src_col_pnum,65535),
                                  p_global =>         p_global );
         -- if we have only target information, that means we want to evaluate the indexes without reference to another table
         -- the index partition type (P_PART_TYPE) and the index type (P_INDEX_TYPE) will default to 'A' (all) when not specified.
      WHEN     p_trg_owner IS NOT NULL
           AND p_trg_table IS NOT NULL
           AND p_src_owner IS NULL
           AND p_src_obj IS NULL
           AND p_src_col IS NULL
         THEN
            etl.unusable_indexes( p_owner =>           p_trg_owner,
                                  p_table =>           p_trg_table,
                                  p_part_name =>       p_part_name,
                                  p_index_type =>      p_index_type,
                                  p_global =>          p_global );
         -- if we don't even have any target information, then all we want to do is set DBMS_APPLICATION_INFORMATION
         -- in this case, we don't modify any indexes.
      WHEN     p_trg_owner IS NULL
           AND p_trg_table IS NULL
           AND p_src_owner IS NULL
           AND p_src_obj IS NULL
           AND p_src_col IS NULL
         THEN
            job.log_msg( 'No indexes will be made unusable' );
         ELSE
            -- if we don't make a match somewhere, then we've made a parameter mistake
            raise_application_error( -20001, 'Incorrect combination of parameters' );
      END CASE;
   END start_map_control;

   PROCEDURE end_map_control(
      p_trg_owner   VARCHAR2 DEFAULT NULL,
      p_trg_table   VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      CASE
         WHEN     p_trg_owner IS NOT NULL
              AND p_trg_table IS NOT NULL
         THEN
            etl.usable_indexes( p_trg_owner, p_trg_table );
         WHEN     p_trg_owner IS NULL
              AND p_trg_table IS NULL
         THEN
            job.log_msg( 'No indexes will be made usable' );
         ELSE
            raise_application_error( -20001, 'Incorrect combination of parameters' );
      END CASE;

      job.log_msg( 'End mapping' );
      g_app.clear_app_info;
   END end_map_control;

   PROCEDURE run_process_flow(
      p_flow_name       VARCHAR2,
      p_flow_location   VARCHAR2 DEFAULT 'OWF_LOCATION' )
   AS
      l_retval   NUMBER;
   BEGIN
      l_retval :=
         owbrep.wb_rt_api_exec.run_task( p_flow_location,
                                         'PROCESSFLOW',
                                         UPPER( p_flow_name ),
                                         '',
                                         '',
                                         1 );
   END run_process_flow;
END owb_utils;
/