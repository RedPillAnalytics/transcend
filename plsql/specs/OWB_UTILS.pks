CREATE OR REPLACE PACKAGE owb_utils
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
	p_part_type      BOOLEAN DEFAULT FALSE,
      );

   PROCEDURE end_map_control
      ( p_owner   VARCHAR2 DEFAULT NULL,
	p_table   VARCHAR2 DEFAULT NULL );

  PROCEDURE run_process_flow
      ( p_flow_name       VARCHAR2,
	p_flow_location   VARCHAR2 DEFAULT 'OWF_LOCATION' );
END owb_utils;
/