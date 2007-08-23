CREATE OR REPLACE PACKAGE td_owbapi AUTHID CURRENT_USER
AS
   PROCEDURE start_map_control(
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
      p_oper_id         NUMBER DEFAULT NULL,
      p_runmode         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE end_map_control(
      p_owner            VARCHAR2 DEFAULT NULL,
      p_table            VARCHAR2 DEFAULT NULL,
      p_source_owner     VARCHAR2 DEFAULT NULL,
      p_source_table     VARCHAR2 DEFAULT NULL,
      p_partname         VARCHAR2 DEFAULT NULL,
      p_idx_tablespace   VARCHAR2 DEFAULT NULL,
      p_index_drop       VARCHAR2 DEFAULT NULL,
      p_handle_fkeys     VARCHAR2 DEFAULT NULL,
      p_statistics       VARCHAR2 DEFAULT NULL,
      p_oper_id          NUMBER DEFAULT NULL,
      p_runmode          VARCHAR2 DEFAULT NULL
   );

   PROCEDURE run_process_flow(
      p_flow_name       VARCHAR2,
      p_flow_location   VARCHAR2,
      p_rep_owner	VARCHAR2,
      p_runmode         VARCHAR2 DEFAULT NULL
   );

END td_owbapi;
/