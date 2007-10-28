CREATE OR REPLACE PACKAGE td_owb AUTHID CURRENT_USER
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
      p_batch_id         NUMBER DEFAULT NULL
   );

   PROCEDURE end_map_control(
      p_owner            VARCHAR2 DEFAULT NULL,
      p_table            VARCHAR2 DEFAULT NULL,
      p_source_owner     VARCHAR2 DEFAULT NULL,
      p_source_table     VARCHAR2 DEFAULT NULL,
      p_partname         VARCHAR2 DEFAULT NULL,
      p_index_space      VARCHAR2 DEFAULT NULL,
      p_index_drop       VARCHAR2 DEFAULT NULL,
      p_statistics       VARCHAR2 DEFAULT NULL
   );

END td_owb;
/