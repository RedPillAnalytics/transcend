CREATE OR REPLACE PACKAGE td_control AUTHID CURRENT_USER
IS
   FUNCTION get_priority( p_accessor VARCHAR2)
      RETURN NUMBER;

   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4
   );

   PROCEDURE set_runmode(
      p_module            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime'
   );

   PROCEDURE set_registration(
      p_module         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'register'
   );

   PROCEDURE set_session_parameter( p_module VARCHAR2, p_name VARCHAR2, p_value VARCHAR2 );
      
   PROCEDURE set_priority(
      p_accessor     VARCHAR2,
      p_priority     NUMBER
   );

   PROCEDURE clear_log(
      p_runmode      VARCHAR2 DEFAULT NULL,
      p_session_id   NUMBER DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' )
   );
END td_control;
/