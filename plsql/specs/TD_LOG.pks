CREATE OR REPLACE PACKAGE td_log AUTHID CURRENT_USER
AS
   g_package_name     VARCHAR2(30);
   g_session_id	      NUMBER         DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   g_instance_name    VARCHAR(30)    DEFAULT SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
   g_machine	      VARCHAR2(50)   DEFAULT SYS_CONTEXT( 'USERENV', 'HOST' )
			     	       	     || '['
				 	     || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         				     || ']';
   g_dbuser	      VARCHAR2(30)   DEFAULT SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
   g_osuser	      VARCHAR2(30)   DEFAULT SYS_CONTEXT( 'USERENV', 'OS_USER' );
   g_client_info      VARCHAR2(30);
   g_module	      VARCHAR2(30);
   g_action	      VARCHAR2(30);
   g_registration     VARCHAR2(30);
   g_logging_level    VARCHAR2(30);
   g_runmode 	      VARCHAR2(10) DEFAULT 'runtime';

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;
END td_log;
/