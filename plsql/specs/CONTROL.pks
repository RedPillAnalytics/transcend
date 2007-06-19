CREATE OR REPLACE PACKAGE control
IS
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
      
   PROCEDURE set_session_parameter(
      p_module   VARCHAR2,
      p_name     VARCHAR2,
      p_value    VARCHAR2
   );
      
   PROCEDURE clear_log(
      p_session_id   NUMBER DEFAULT sys_context('USERENV','SESSIONID'),
      p_runmode      VARCHAR2 DEFAULT 'debug'
   );

END control;
/