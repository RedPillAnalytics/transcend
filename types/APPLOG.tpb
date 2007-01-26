CREATE OR REPLACE TYPE BODY efw.applog
AS
   CONSTRUCTOR FUNCTION applog(
      p_action        VARCHAR2 DEFAULT 'Begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_register      BOOLEAN DEFAULT TRUE,
      p_debug         BOOLEAN DEFAULT FALSE )
      RETURN SELF AS RESULT
   AS
   BEGIN
      -- get the session id
      session_id := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      
      IF p_register
      THEN
	 -- read previous app_info settings
	 -- if not registering with oracle, then this is not necessary
	 DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
	 DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      END IF;

      -- populate attributes with new app_info settings
      client_info := NVL( p_client_info, prev_client_info );
      module :=
         CASE
            WHEN     p_debug
                 AND p_module IS NOT NULL
               THEN p_module || ' (DEBUG)'
            WHEN     NOT p_debug
                 AND p_module IS NOT NULL
               THEN p_module
            WHEN     p_debug
                 AND p_module IS NULL
               THEN get_package_name || ' (DEBUG)'
            ELSE get_package_name
         END;
      action := p_action;
      -- set other attributes
      instance_name := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
      dbuser := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
      osuser := SYS_CONTEXT( 'USERENV', 'OS_USER' );
      machine :=
              SYS_CONTEXT( 'USERENV', 'HOST' ) || '[' || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
              || ']';
	 
	 -- register the application with oracle
      IF p_register
      THEN
         DBMS_APPLICATION_INFO.set_client_info( client_info );
         DBMS_APPLICATION_INFO.set_module( module, action );
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
         RAISE;
   END applog;
   -- used to pull the calling block from the dictionary
   -- used to populate CALL_STACK column in the LOG_TABLE
   MEMBER FUNCTION whence
      RETURN VARCHAR2
   AS
      l_call_stack    VARCHAR2( 4096 ) DEFAULT DBMS_UTILITY.format_call_stack || CHR( 10 );
      l_num           NUMBER;
      l_found_stack   BOOLEAN          DEFAULT FALSE;
      l_line          VARCHAR2( 255 );
      l_cnt           NUMBER           := 0;
   BEGIN
      LOOP
         l_num := INSTR( l_call_stack, CHR( 10 ));
         EXIT WHEN(    l_cnt = 4
                    OR l_num IS NULL
                    OR l_num = 0 );
         l_line := SUBSTR( l_call_stack,
                           1,
                           l_num - 1 );
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
   -- formats the results of WHENCE function to pull out the current package name
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2
   AS
      l_package_name   log_table.call_stack%TYPE;
   BEGIN
      l_package_name := REGEXP_SUBSTR( whence,
                                       '\\S+$',
                                       1,
                                       1,
                                       'i' );
      RETURN l_package_name;
   END get_package_name;
   MEMBER PROCEDURE set_action(
      p_action   VARCHAR2 )
   AS
   BEGIN
      DBMS_APPLICATION_INFO.set_action( p_action );
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END set_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      DBMS_APPLICATION_INFO.set_client_info( prev_client_info );
      DBMS_APPLICATION_INFO.set_module( prev_module, prev_action );
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END clear_app_info;
   -- used to write a standard message to the LOG_TABLE
   MEMBER PROCEDURE log_msg(
      p_msg   VARCHAR2 )           -- P_MSG is simply the text that will be written to the LOG_TABLE
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_code     NUMBER                        DEFAULT SQLCODE;
      l_msg      log_table.msg%TYPE;
      l_scn      v$database.current_scn%TYPE;
   BEGIN
      -- still write as much to the logfile if we can even if it's too large for the log table
      BEGIN
         l_msg := p_msg;
      EXCEPTION
         WHEN VALUE_ERROR
         THEN
            l_msg := SUBSTR( l_msg,
                             0,
                             1998 ) || '>>';
      END;

      -- find out what called me
      l_whence := whence;

      -- get the current_scn
      SELECT current_scn
        INTO l_scn
        FROM v$database;

      -- write the record to the log table
      INSERT INTO log_table
                  ( entry_ts,
                    msg,
                    client_info,
                    module,
                    action,
                    session_id,
                    current_scn,
                    instance_name,
                    machine,
                    dbuser,
                    osuser,
                    code,
                    call_stack,
                    back_trace )
           VALUES ( SYSTIMESTAMP,
                    l_msg,
                    NVL( SELF.client_info, 'Not Set' ),
                    NVL( SELF.module, 'Not Set' ),
                    NVL( SELF.action, 'Not Set' ),
                    SELF.session_id,
                    l_scn,
                    SELF.instance_name,
                    SELF.machine,
                    SELF.dbuser,
                    SELF.osuser,
                    l_code,
                    l_whence,
                    REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace,
                                            1,
                                            4000 ),
                                    '[[:cntrl:]]',
                                    '; ' ));

      COMMIT;
      -- also output the message to the screen
      -- the client can control whether or not they want to see this
      -- in sqlplus, just SET SERVEROUTPUT ON or OFF
      DBMS_OUTPUT.put_line( p_msg );
   END log_msg;
   MEMBER PROCEDURE log_err
   AS
      l_msg   VARCHAR2( 1020 ) DEFAULT SQLERRM;
   BEGIN
      log_msg( l_msg );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
   END log_err;
   MEMBER PROCEDURE log_cnt_msg(
      p_count   NUMBER,
      p_msg     VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      log_cnt( p_count );
      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected: ' || p_count ));
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END log_cnt_msg;
   MEMBER PROCEDURE log_cnt(
      p_count   NUMBER )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_client_info     VARCHAR2( 64 );
      l_module          VARCHAR2( 48 );
      l_action          VARCHAR2( 32 );
      l_sessionid       NUMBER;
      l_instance_name   VARCHAR2( 30 );
   BEGIN
      DBMS_APPLICATION_INFO.read_client_info( l_client_info );
      DBMS_APPLICATION_INFO.read_module( l_module, l_action );
      l_sessionid := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      l_instance_name := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );

      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  ( entry_ts,
                    client_info,
                    module,
                    action,
                    session_id,
                    row_cnt )
           VALUES ( SYSTIMESTAMP,
                    SELF.client_info,
                    SELF.module,
                    SELF.action,
                    SELF.session_id,
                    p_count );

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END log_cnt;
END;
/