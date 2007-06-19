
CREATE OR REPLACE PACKAGE BODY owb_api
AS
   g_app applog;

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
      )
   AS
   BEGIN
      g_app := applog( p_action => 'start OWB mapping');
      g_app.log_msg( 'Beginning OWB mapping' );

      -- see whether or not to call UNUSABLE_INDEXES
      IF     p_owner IS NOT NULL
         AND p_table IS NOT NULL
      THEN
         transcend.unusable_indexes( p_owner =>          p_owner,
                                     p_table =>          p_table,
				     p_partname =>	 p_partname,
				     p_source_owner =>	 p_source_owner,
				     p_source_object =>	 p_source_object,
				     p_source_column =>	 p_source_column,
				     p_d_num =>          nvl(p_d_num,0),
				     p_p_num =>          nvl(p_p_num,65535),
				     p_index_regexp	 p_index_regexp,
				     p_index_type	 p_index_type,
				     p_part_type =>	 p_part_type );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_app.log_err;
         RAISE;
   END start_map_control;

   PROCEDURE end_map_control
      ( p_owner   VARCHAR2 DEFAULT NULL,
	p_table   VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      g_app.set_action('end OWB mapping');
      IF p_owner IS NULL AND p_table IS NULL
      THEN
	 transcend.usable_indexes( p_owner, p_table);
      END IF;

      g_app.log_msg( 'Ending OWB mapping' );      
      g_app.clear_app_info;
   END end_map_control;

   PROCEDURE run_process_flow
      ( p_flow_name       VARCHAR2,
	p_flow_location   VARCHAR2 DEFAULT 'OWF_LOCATION' )
   AS
      l_retval   NUMBER;
   BEGIN
      l_retval :=
      owbrep.wb_rt_api_exec.run_task( p_flow_location,
                                      'PROCESSFLOW',
                                      upper( p_flow_name ),
                                      '',
                                      '',
                                      1 );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_app.log_err;
         RAISE;
   END run_process_flow;
END owb_api;
/