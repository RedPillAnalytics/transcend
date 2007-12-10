CREATE OR REPLACE PACKAGE BODY td_inst
AS
-- global variables placed in the package body because they should be accessed or set outside the package

   -- variables for holding information about the current session
   g_service_name    VARCHAR2( 64 ) := SYS_CONTEXT( 'USERENV', 'SERVICE_NAME' );
   g_session_id      NUMBER         := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   g_instance_name   VARCHAR( 30 )  := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
   g_machine         VARCHAR2( 50 )
      :=    SYS_CONTEXT( 'USERENV', 'HOST' )
         || '['
         || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         || ']';
   g_dbuser          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
   g_osuser          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'OS_USER' );
-- variables for holding information used to register an application with some other framework, such as DBMS_APPLCIATION_INFO
   g_client_info     VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'CLIENT_INFO' );
   g_module          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'MODULE' );
   g_action          VARCHAR2( 30 ) := SYS_CONTEXT( 'USERENV', 'ACTION' );
   g_batch_id        NUMBER;
   g_registration    VARCHAR2( 30 ) := 'appinfo';
   g_logging_level   VARCHAR2( 30 ) := 2;
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
            -- now set the new values
            DBMS_APPLICATION_INFO.set_client_info( g_client_info );
            DBMS_APPLICATION_INFO.set_module( g_module, g_action );
      END CASE;
   END REGISTER;

   -- DEFAULT ACCESSOR METHODS

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

   -- used to return a distinct error message number by label
   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER
   AS
      l_code   error_conf.code%TYPE;
   BEGIN
      BEGIN
         SELECT (0 - code)
           INTO l_code
           FROM error_conf
          WHERE NAME = p_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( -20001, 'The specified error has not been configured: '||p_name );
      END;

      RETURN l_code;
   END get_err_cd;

   -- used to return a distinct error message text string by label
   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_msg   error_conf.MESSAGE%TYPE;
   BEGIN
      BEGIN
         SELECT MESSAGE
           INTO l_msg
           FROM error_conf
          WHERE NAME = p_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( -20001, 'The specified error has not been configured: '||p_name );
      END;

      RETURN l_msg;
   END get_err_msg;


   -- OTHER PROGRAM UNITS

   -- used to pull the calling block from the dictionary
   -- used to populate CALL_STACK column in the LOG_TABLE
   FUNCTION whence
      RETURN VARCHAR2
   AS
      l_call_stack    VARCHAR2( 4096 )
                                      DEFAULT DBMS_UTILITY.format_call_stack || CHR( 10 );
      l_num           NUMBER;
      l_found_stack   BOOLEAN          DEFAULT FALSE;
      l_line          VARCHAR2( 255 );
      l_cnt           NUMBER           := 0;
   BEGIN
      LOOP
         l_num := INSTR( l_call_stack, CHR( 10 ));
         EXIT WHEN( l_cnt = 4 OR l_num IS NULL OR l_num = 0 );
         l_line := SUBSTR( l_call_stack, 1, l_num - 1 );
         l_call_stack := SUBSTR( l_call_stack, l_num + 1 );

         IF ( NOT l_found_stack )
         THEN
            IF ( l_line LIKE '%handle%number%name%' )
            THEN
               l_found_stack := TRUE;
            END IF;
         ELSE
            l_cnt := l_cnt + 1;
         END IF;
      END LOOP;

      RETURN l_line;
   END whence;

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