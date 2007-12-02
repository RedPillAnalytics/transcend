CREATE OR REPLACE PACKAGE td_inst AUTHID CURRENT_USER
AS
   PROCEDURE REGISTER;

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
      
   FUNCTION is_full_debugmode
      RETURN BOOLEAN;

   FUNCTION is_registered
      RETURN BOOLEAN;

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;
   
   FUNCTION whence
      RETURN VARCHAR2;

   PROCEDURE set_scheduler_info(
      p_session_id  NUMBER,
      p_module	    VARCHAR2,
      p_action	    varchar2
   );

END td_inst;
/