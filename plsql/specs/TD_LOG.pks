CREATE OR REPLACE PACKAGE td_log AUTHID CURRENT_USER
AS
   PROCEDURE begin_debug;
   
   PROCEDURE end_debug;
      
   FUNCTION is_debugmode
      RETURN BOOLEAN;

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;      
      
END td_log;
/