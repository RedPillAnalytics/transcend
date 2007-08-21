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
      
   FUNCTION is_debugmode
      RETURN BOOLEAN;
   
   -- return a Boolean determing full debug mode
   FUNCTION is_full_debugmode
      RETURN BOOLEAN;

   -- get method for a Boolean to determine registration
   FUNCTION is_registered
      RETURN BOOLEAN;

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;
      
   FUNCTION whence
      RETURN VARCHAR2;

   PROCEDURE log_msg(
      p_msg      VARCHAR2,
      p_level    NUMBER DEFAULT 2,
      p_stdout   VARCHAR2 DEFAULT 'yes'
   );

   PROCEDURE log_err;

   PROCEDURE log_cnt_msg(
      p_count     NUMBER,
      p_msg       VARCHAR2 DEFAULT NULL,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   );
      
   PROCEDURE start_debug;

   PROCEDURE stop_debug;

END td_inst;
/