CREATE OR REPLACE PACKAGE owb_api
AS
   PROCEDURE start_map_control
      ( p_owner          VARCHAR2 DEFAULT NULL,
	p_table          VARCHAR2 DEFAULT NULL,
	p_partname       VARCHAR2 DEFAULT NULL,
	p_source_owner   VARCHAR2 DEFAULT NULL,
	p_source_object  VARCHAR2 DEFAULT NULL,
	p_source_column  VARCHAR2 DEFAULT NULL,
	p_d_num   NUMBER DEFAULT NULL,
	p_p_num   NUMBER DEFAULT NULL,
	p_index_regexp   VARCHAR2 DEFAULT NULL,
	p_index_type     VARCHAR2 DEFAULT NULL,
	p_part_type      VARCHAR2 DEFAULT NULL,
	p_oper_id	 NUMBER DEFAULT NULL
      );

   PROCEDURE end_map_control
      ( p_owner   VARCHAR2 DEFAULT NULL,
	p_table   VARCHAR2 DEFAULT NULL,
	p_oper_id NUMBER DEFAULT NULL );

END owb_api;
/