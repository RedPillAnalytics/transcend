CREATE OR REPLACE PACKAGE BODY td_inst
AS
   -- global variables placed in the package body because they should be accessed or set outside the package
   g_package_name     VARCHAR2(30) := nvl($$plsql_unit,'NA');
   g_session_id	      NUMBER       := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   g_instance_name    VARCHAR(30)  := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
   g_machine	      VARCHAR2(50) := SYS_CONTEXT( 'USERENV', 'HOST' )
			     	       	     || '['
				 	     || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         				     || ']';
   g_dbuser	      VARCHAR2(30) := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
   g_osuser	      VARCHAR2(30) := SYS_CONTEXT( 'USERENV', 'OS_USER' );
   g_client_info      VARCHAR2(30) := nvl(sys_context('USERENV','CLIENT_INFO'),'NA');
   g_module           VARCHAR2(30) := nvl(sys_context('USERENV','MODULE'),'NA');
   g_action           VARCHAR2(30) := nvl(sys_context('USERENV','ACTION'),'NA');
   g_batch_id	      NUMBER;
   g_registration     VARCHAR2(30) := 'appinfo';
   g_logging_level    VARCHAR2(30) := 4;
   g_runmode 	      VARCHAR2(10) := 'runtime';

   -- begins debug mode
   PROCEDURE begin_debug
   AS
   BEGIN
      g_runmode := 'debug';
   END begin_debug;

   -- begins debug mode
   PROCEDURE end_debug
   AS
   BEGIN
      g_runmode := 'debug';
   END end_debug;
   
   -- registers the application
   PROCEDURE register
   AS
   BEGIN
      CASE registration
      WHEN 'NA'
      THEN
      NULL;
      WHEN 'appinfo'
      THEN
         -- now set the new values
         DBMS_APPLICATION_INFO.set_client_info( g_client_info );
         DBMS_APPLICATION_INFO.set_module( g_module, g_action );
      END CASE;
   END register;
      
   -- returns Boolean for determining runmode
   FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE g_runmode
         WHEN 'debug'
            THEN TRUE
         ELSE FALSE
      END;
   END is_debugmode;   

   -- DEFAULT ACCESSOR METHODS
   FUNCTION runmode
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_runmode;
   END runmode;      
   
   PROCEDURE runmode ( p_runmode VARCHAR2 )
   AS
   BEGIN
      g_runmode := p_runmode;
   END runmode;      
   
   FUNCTION registration
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_registration;
   END registration;      
   
   PROCEDURE registration ( p_registration VARCHAR2 )
   AS
   BEGIN
      g_registration := p_registration;
   END registration;

   FUNCTION logging_level
      RETURN NUMBER 
   AS
   BEGIN
      RETURN g_logging_level;
   END logging_level;      
   
   PROCEDURE logging_level ( p_logging_level NUMBER )
   AS
   BEGIN
      g_logging_level := p_logging_level;
   END logging_level;

   FUNCTION batch_id
      RETURN NUMBER 
   AS
   BEGIN
      RETURN g_batch_id;
   END batch_id;      
   
   PROCEDURE batch_id ( p_batch_id NUMBER )
   AS
   BEGIN
      g_batch_id := p_batch_id;
   END batch_id;
   
   -- MODIFIED ACCESSOR METHODS
   
   FUNCTION module
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_module;
   END module;
   
   -- have to register the application
   PROCEDURE module ( p_module VARCHAR2 )
   AS
   BEGIN
      g_module := p_module;
      register;
   END module;
   
   FUNCTION action
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_action;
   END action;
   
   -- have to register the application
   PROCEDURE action ( p_action VARCHAR2 )
   AS
   BEGIN
      g_action := p_action;
      register;
   END action;
   
   FUNCTION client_info
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_client_info;
   END client_info;
   
   -- have to register the application
   PROCEDURE client_info ( p_client_info VARCHAR2 )
   AS
   BEGIN
      g_client_info := p_client_info;
      register;
   END client_info;

   -- CUSTOM AMETHODS
   -- used to return a distinct error message number by label
   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER
   AS
      l_code   err_cd.code%TYPE;
   BEGIN
      SELECT code
        INTO l_code
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_code;
   END get_err_cd;
   
   -- used to return a distinct error message text string by label
   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_msg   err_cd.MESSAGE%TYPE;
   BEGIN
      SELECT MESSAGE
        INTO l_msg
        FROM err_cd
       WHERE NAME = p_name;

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

   -- used to write a standard message to the LOG_TABLE
   PROCEDURE log_msg(
      p_msg       VARCHAR2,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes'
   )
   -- P_MSG is simply the text that will be written to the LOG_TABLE
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_code     NUMBER                        DEFAULT SQLCODE;
      l_msg      log_table.msg%TYPE;
      l_scn      NUMBER;
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   
   BEGIN
      -- still write as much to the logfile if we can even if it's too large for the log table
      BEGIN
         l_msg := p_msg;
      EXCEPTION
         WHEN VALUE_ERROR
         THEN
            l_msg := SUBSTR( l_msg, 0, 1998 ) || '>>';
      END;

      -- find out what called me
      l_whence := whence;
      
      -- using invokers rights model
      -- some users won't have access to see the SCN
      -- need to except this just in case
      -- if cannot see the scn, then use a 0
      BEGIN
	 SELECT current_scn
           INTO l_scn
           FROM v$database;
      EXCEPTION
         WHEN e_no_tab
         THEN
            l_scn := 0;
      END;
      
      -- check to see the logging level to see if the message should be written
      IF logging_level >= p_level
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module,
                       action, runmode, session_id, current_scn,
                       instance_name, machine, dbuser, osuser,
                       code, call_stack,
                       back_trace,
                       oper_id
                     )
              VALUES ( l_msg, g_client_info, g_module,
                       g_action, g_runmode, g_session_id, l_scn,
                       g_instance_name, g_machine, g_dbuser, g_osuser,
                       l_code, l_whence,
                       REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace,
                                               1,
                                               4000
                                             ),
                                       '[[:cntrl:]]',
                                       '; '
                                     ),
                       g_batch_id
                     );

         COMMIT;

	 -- also output the message to the screen
	 -- the client can control whether or not they want to see this
	 -- in sqlplus, just SET SERVEROUTPUT ON or OFF
	 -- by default, all messages are logged to STDOUT
	 -- this can be controlled per message with P_STDOUT, which defaults to 'yes'
	 IF td_ext.is_true( p_stdout )
	 THEN
            DBMS_OUTPUT.put_line( p_msg );
	 END IF;
      END IF;
   END log_msg;

   PROCEDURE log_err
   AS
      l_msg   VARCHAR2( 1020 ) DEFAULT SQLERRM;
   BEGIN
      log_msg( l_msg, 1, 'no' );
   END log_err;

   PROCEDURE log_cnt_msg(
      p_count     NUMBER,
      p_msg       VARCHAR2 DEFAULT NULL,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  ( client_info, module,
                    action, runmode, session_id, row_cnt
                  )
           VALUES ( NVL( g_client_info, 'NA' ), NVL( g_module, 'NA' ),
                    NVL( g_action, 'NA' ), g_runmode, g_session_id, p_count
                  );

      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected' ) || ': ' || p_count,
               p_level,
               p_stdout
             );
      COMMIT;
   END log_cnt_msg;
   BEGIN
      register;
END td_inst;
/