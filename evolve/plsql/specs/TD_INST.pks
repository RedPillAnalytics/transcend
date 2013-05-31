CREATE OR REPLACE PACKAGE td_inst AUTHID CURRENT_USER
AS

   current_action CONSTANT VARCHAR2(30) := '#*current_action*#';

   PROCEDURE REGISTER;
      
   FUNCTION service_name
      RETURN VARCHAR2;

   PROCEDURE service_name( p_service_name VARCHAR2 );
      
   FUNCTION instance_name
      RETURN VARCHAR2;

   PROCEDURE instance_name( p_instance_name VARCHAR2 );
   
   FUNCTION machine
      RETURN VARCHAR2;

   PROCEDURE machine( p_machine VARCHAR2 );
   
   FUNCTION dbuser
      RETURN VARCHAR2;

   PROCEDURE dbuser( p_dbuser VARCHAR2 );

   FUNCTION osuser
      RETURN VARCHAR2;

   PROCEDURE osuser( p_osuser VARCHAR2 );

   FUNCTION starttime
      RETURN VARCHAR2;

   PROCEDURE starttime( p_starttime VARCHAR2 );
   
   FUNCTION runmode
      RETURN VARCHAR2;

   PROCEDURE runmode( p_runmode VARCHAR2 );

   FUNCTION registration
      RETURN VARCHAR2;

   PROCEDURE registration( p_registration VARCHAR2 );

   FUNCTION logging_level
      RETURN NUMBER;

   PROCEDURE logging_level( p_logging_level NUMBER );

   FUNCTION module
      RETURN VARCHAR2;

   PROCEDURE module( p_module VARCHAR2 );

   FUNCTION action
      RETURN VARCHAR2;

   PROCEDURE action( p_action VARCHAR2 );

   FUNCTION client_info
      RETURN VARCHAR2;

   PROCEDURE client_info( p_client_info VARCHAR2 );

   FUNCTION batch_id
      RETURN NUMBER;

   PROCEDURE batch_id( p_batch_id NUMBER );

   FUNCTION session_id
      RETURN NUMBER;

   PROCEDURE session_id( p_session_id NUMBER );
      
   FUNCTION is_full_debugmode
      RETURN BOOLEAN;

   FUNCTION is_registered
      RETURN BOOLEAN;

   FUNCTION get_elapsed_time
      RETURN NUMBER;

   PROCEDURE set_scheduler_info(
      p_session_id  NUMBER,
      p_module	    VARCHAR2,
      p_action	    varchar2
   );

END td_inst;
/