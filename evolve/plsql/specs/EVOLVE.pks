CREATE OR REPLACE PACKAGE evolve AUTHID CURRENT_USER
AS
   
   FUNCTION get_action
      RETURN VARCHAR2;

   FUNCTION get_module
      RETURN VARCHAR2;

   PROCEDURE log_msg( p_msg VARCHAR2, p_level NUMBER DEFAULT 1 );

   PROCEDURE log_results_msg( 
      p_count       NUMBER,
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_category    VARCHAR2,
      p_msg         VARCHAR2 DEFAULT NULL, 
      p_level       NUMBER   DEFAULT 1
   );

   PROCEDURE log_err;
      
   PROCEDURE log_variable( 
      p_name       VARCHAR2,
      p_value      VARCHAR2
   );

   PROCEDURE raise_err( p_name VARCHAR2, p_add_msg VARCHAR2 DEFAULT NULL );

   PROCEDURE print_query( p_query IN VARCHAR2 );

   FUNCTION is_debugmode
      RETURN BOOLEAN;

   PROCEDURE start_debug;

   PROCEDURE stop_debug;
   
   FUNCTION exec_sql( p_sql VARCHAR2, p_msg VARCHAR2 DEFAULT NULL, p_auto VARCHAR2 DEFAULT 'no' )
      RETURN NUMBER;

   PROCEDURE exec_sql(
      p_sql             VARCHAR2,
      p_msg             VARCHAR2 DEFAULT NULL,
      p_auto            VARCHAR2 DEFAULT 'no',
      p_concurrent_id   VARCHAR2 DEFAULT NULL
   );

   FUNCTION get_concurrent_id
      RETURN VARCHAR2;

   PROCEDURE submit_sql(
      p_sql             VARCHAR2,
      p_concurrent_id   VARCHAR2,
      p_job_class       VARCHAR2 DEFAULT 'EVOLVE_DEFAULT_CLASS'
   );

   PROCEDURE coordinate_sql(
      p_concurrent_id   VARCHAR2,
      p_raise_err       VARCHAR2 DEFAULT 'yes',
      p_sleep           NUMBER DEFAULT 5,
      p_timeout         NUMBER DEFAULT 0
   );

   PROCEDURE consume_sql( p_session_id NUMBER, p_module VARCHAR2, p_action VARCHAR2, p_sql VARCHAR2 );
      
   PROCEDURE dump_log( p_directory VARCHAR2, p_repository VARCHAR2, p_dump_type VARCHAR2 DEFAULT 'session' );

END evolve;
/