CREATE OR REPLACE PACKAGE BODY td_inst
AS
   -- global variables placed in the package body because they should be accessed or set outside the package

   -- variables for holding information about the current session
   g_service_name    VARCHAR2( 64 ) := SYS_CONTEXT( 'USERENV', 'SERVICE_NAME' );
   g_session_id      NUMBER         := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   g_instance_name   VARCHAR( 30 )  := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
   g_machine         VARCHAR2( 64 ) := SYS_CONTEXT( 'USERENV', 'HOST' );
   g_dbuser          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
   g_osuser          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'OS_USER' );
   -- variables for holding information used to register an application with some other framework, such as DBMS_APPLCIATION_INFO
   g_client_info     VARCHAR2( 64 ) := SYS_CONTEXT( 'USERENV', 'CLIENT_INFO' );
   g_module          VARCHAR2( 48 ) := SYS_CONTEXT( 'USERENV', 'MODULE' );
   g_action          VARCHAR2( 32 ) := SYS_CONTEXT( 'USERENV', 'ACTION' );
   -- miscelaneous other variables for enhanced framework functionality
   g_starttime       DATE;
   g_batch_id        NUMBER;
   g_registration    VARCHAR2( 30 ) := 'appinfo';
   g_logging_level   NUMBER         := 2;
   g_runmode         VARCHAR2( 10 ) := 'runtime';

   -- registers the application
   PROCEDURE REGISTER
   AS
   BEGIN
      CASE registration
         WHEN 'noregister'
         THEN
            NULL;
         WHEN 'appinfo'
         THEN
            -- no matter what, we want to change client info   
            DBMS_APPLICATION_INFO.set_client_info( g_client_info );

            -- these are our new values
            DBMS_APPLICATION_INFO.set_module( g_module, g_action );

      END CASE;
   END REGISTER;

   -- DEFAULT ACCESSOR METHODS

   -- accessor methods for starttime
   FUNCTION starttime
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_starttime;
   END starttime;

   PROCEDURE starttime( p_starttime VARCHAR2 )
   AS
   BEGIN
      g_starttime := p_starttime;
   END starttime;

   -- accessor methods for osuser
   FUNCTION osuser
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_osuser;
   END osuser;

   PROCEDURE osuser( p_osuser VARCHAR2 )
   AS
   BEGIN
      g_osuser := p_osuser;
   END osuser;

   -- accessor methods for dbuser
   FUNCTION dbuser
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_dbuser;
   END dbuser;

   PROCEDURE dbuser( p_dbuser VARCHAR2 )
   AS
   BEGIN
      g_dbuser := p_dbuser;
   END dbuser;

   -- accessor methods for machine
   FUNCTION machine
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_machine;
   END machine;

   PROCEDURE machine( p_machine VARCHAR2 )
   AS
   BEGIN
      g_machine := p_machine;
   END machine;

   -- accessor methods for instance_name
   FUNCTION instance_name
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_instance_name;
   END instance_name;

   PROCEDURE instance_name( p_instance_name VARCHAR2 )
   AS
   BEGIN
      g_instance_name := p_instance_name;
   END instance_name;

   -- accessor methods for service_name
   FUNCTION service_name
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_service_name;
   END service_name;

   PROCEDURE service_name( p_service_name VARCHAR2 )
   AS
   BEGIN
      g_service_name := p_service_name;
   END service_name;

   -- accessor methods for runmode
   FUNCTION runmode
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_runmode;
   END runmode;

   PROCEDURE runmode( p_runmode VARCHAR2 )
   AS
   BEGIN
      g_runmode := p_runmode;
   END runmode;

   -- accessor methods for registration
   FUNCTION registration
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_registration;
   END registration;

   PROCEDURE registration( p_registration VARCHAR2 )
   AS
   BEGIN
      g_registration := p_registration;
   END registration;

   -- accessor methods for logging_level
   FUNCTION logging_level
      RETURN NUMBER
   AS
   BEGIN
      RETURN g_logging_level;
   END logging_level;

   PROCEDURE logging_level( p_logging_level NUMBER )
   AS
   BEGIN
      g_logging_level := p_logging_level;
   END logging_level;
      
   -- accessor methods for batch_id
   FUNCTION batch_id
      RETURN NUMBER
   AS
   BEGIN
      RETURN g_batch_id;
   END batch_id;

   PROCEDURE batch_id( p_batch_id NUMBER )
   AS
   BEGIN
      g_batch_id := p_batch_id;
   END batch_id;

   -- accessor methods for session_id
   FUNCTION session_id
      RETURN NUMBER
   AS
   BEGIN
      RETURN g_session_id;
   END session_id;

   PROCEDURE session_id( p_session_id NUMBER )
   AS
   BEGIN
      g_session_id := p_session_id;
   END session_id;

   -- accessor methods for module
   FUNCTION module
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_module;
   END module;

   PROCEDURE module( p_module VARCHAR2 )
   AS
   BEGIN
      g_module := p_module;
   END module;

   -- accessor methods for action
   FUNCTION action
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_action;
   END action;

   PROCEDURE action( p_action VARCHAR2 )
   AS
   BEGIN
      g_action := p_action;
   END action;

   -- accessor methods for client_info
   FUNCTION client_info
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_client_info;
   END client_info;

   PROCEDURE client_info( p_client_info VARCHAR2 )
   AS
   BEGIN
      g_client_info := p_client_info;
   END client_info;   

   -- return a Boolean determing full debug mode
   FUNCTION is_full_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE td_inst.runmode
         WHEN 'full debug'
            THEN TRUE
         ELSE FALSE
      END;
   END is_full_debugmode;

   -- get method for a Boolean to determine registration
   FUNCTION is_registered
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE td_inst.registration
         WHEN 'noregister'
            THEN FALSE
         ELSE TRUE
      END;
   END is_registered;

   -- CUSTOM METHODS

   -- OTHER PROGRAM UNITS
   -- provide elapsed time since the process started
   FUNCTION get_elapsed_time
      RETURN NUMBER
   AS
      l_starttime DATE := starttime;
   BEGIN
      RETURN (SYSDATE - l_starttime)*24*60*60;
   END get_elapsed_time;

   -- the standard methods to set up the session aren't applicable for those submitted in the background with DBMS_SCHEDULER
   -- that is why this method has to be used
   PROCEDURE set_scheduler_info( p_session_id NUMBER, p_module VARCHAR2, p_action VARCHAR2 )
   AS
   BEGIN
      -- set the session information that usually gets set by EVOLVE_OT
      session_id( p_session_id );
      module( p_module );
      action( p_action );

      -- now register the information
      register;

   END set_scheduler_info;
END td_inst;
/

SHOW errors