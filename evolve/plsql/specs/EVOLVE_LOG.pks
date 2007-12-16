CREATE OR REPLACE PACKAGE evolve_log AUTHID CURRENT_USER
AS
   PROCEDURE log_msg( p_msg VARCHAR2, p_level NUMBER DEFAULT 2 );

   PROCEDURE log_cnt_msg( p_count NUMBER, p_msg VARCHAR2 DEFAULT NULL, p_level NUMBER DEFAULT 2 );

   PROCEDURE log_err;

   PROCEDURE raise_err( p_name VARCHAR2, p_add_msg VARCHAR2 DEFAULT NULL );

   PROCEDURE print_query( p_query IN VARCHAR2 );

   FUNCTION is_debugmode
      RETURN BOOLEAN;

   PROCEDURE start_debug;

   PROCEDURE stop_debug;
END evolve_log;
/