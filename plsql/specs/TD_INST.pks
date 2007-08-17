CREATE OR REPLACE PACKAGE td_inst AUTHID CURRENT_USER
AS
   PROCEDURE register;

   FUNCTION runmode
      RETURN VARCHAR2;      
   
   PROCEDURE runmode ( p_runmode VARCHAR2 );
	
   FUNCTION runmode_priority
      RETURN NUMBER;      
   
   PROCEDURE runmode_priority ( p_runmode_priority NUMBER );      

   FUNCTION registration
      RETURN VARCHAR2;      
   
   PROCEDURE registration ( p_registration VARCHAR2 );

   FUNCTION registration_priority
      RETURN NUMBER;      
   
   PROCEDURE registration_priority ( p_registration_priority NUMBER );

   FUNCTION logging_level
      RETURN NUMBER;      
   
   PROCEDURE logging_level ( p_logging_level NUMBER );

   FUNCTION logging_level_priority
      RETURN NUMBER;      
   
   PROCEDURE logging_level_priority ( p_logging_level_priority NUMBER );

   FUNCTION module
      RETURN VARCHAR2;      
   
   PROCEDURE module ( p_module VARCHAR2 );

   FUNCTION module_priority
      RETURN NUMBER;      
   
   PROCEDURE module_priority ( p_module_priority NUMBER );
      
   FUNCTION action
      RETURN VARCHAR2;      
   
   PROCEDURE action ( p_action VARCHAR2 );

   FUNCTION action_priority
      RETURN NUMBER;      
   
   PROCEDURE action_priority ( p_action_priority NUMBER );      

   FUNCTION client_info
      RETURN VARCHAR2;      
   
   PROCEDURE client_info ( p_client_info VARCHAR2 );

   FUNCTION client_info_priority
      RETURN NUMBER;      
   
   PROCEDURE client_info_priority ( p_client_info_priority NUMBER );

   FUNCTION batch_id
      RETURN NUMBER;      
   
   PROCEDURE batch_id ( p_batch_id NUMBER );

   FUNCTION batch_id_priority
      RETURN NUMBER;      
   
   PROCEDURE batch_id_priority ( p_batch_id_priority NUMBER );      

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;
      
   PROCEDURE log_msg(
      p_msg       VARCHAR2,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes'
   );
   
   PROCEDURE log_err;

   PROCEDURE log_cnt_msg(
      p_count     NUMBER,
      p_msg       VARCHAR2 DEFAULT NULL,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   );

   FUNCTION is_runmode_priority ( p_attribute VARCHAR2 )
     RETURN BOOLEAN;
      
END td_inst;
/