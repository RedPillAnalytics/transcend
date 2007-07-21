CREATE OR REPLACE TYPE BODY logtype
AS
   CONSTRUCTOR FUNCTION logtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_register      VARCHAR2 DEFAULT 'yes',
      p_runmode       VARCHAR2 DEFAULT 'runtime'
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      -- get the session id
      session_id := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      -- first we need to populate the module attribute, because it helps us determine parameter values
      SELF.set_module( p_module );
      -- we also set the action, which may be used one day to fine tune parameters
      SELF.set_action( p_action );
      -- now we can use the MODULE attribute to get the runmode
      SELF.set_runmode( p_runmode );
      -- set the registration
      SELF.set_register( p_register );

      -- if we are registering, then we need to save the old values
      IF REGISTER = 'yes'
      THEN
         -- read previous app_info settings
         -- if not registering with oracle, then this is not necessary
         DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
         DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      END IF;

      -- populate attributes with new app_info settings
      client_info := NVL( p_client_info, prev_client_info );
      -- get information about the session for logging purposes
      set_session_info;

      IF REGISTER = 'yes'
      THEN
         -- now set the new values
         DBMS_APPLICATION_INFO.set_client_info( client_info );
         DBMS_APPLICATION_INFO.set_module( module, action );
      END IF;

      RETURN;
   END logtype;
   MEMBER PROCEDURE set_session_info
   AS
   BEGIN
      -- set other attributes
      instance_name := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
      dbuser := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
      osuser := SYS_CONTEXT( 'USERENV', 'OS_USER' );
      machine :=
            SYS_CONTEXT( 'USERENV', 'HOST' )
         || '['
         || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         || ']';
   END set_session_info;
   -- used to pull the calling block from the dictionary
   -- used to populate CALL_STACK column in the LOG_TABLE
   MEMBER FUNCTION whence
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
   MEMBER PROCEDURE log_msg(
      p_msg       VARCHAR2,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   )
   -- P_MSG is simply the text that will be written to the LOG_TABLE
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_code     NUMBER               DEFAULT SQLCODE;
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
      -- need to use dynamic sql to make sure compilation is not an issue
      -- if cannot see the scn, then use a 0
      BEGIN
         EXECUTE IMMEDIATE 'select current_scn from v$database'
                      INTO l_scn;
      EXCEPTION
         WHEN e_no_tab
         THEN
            l_scn := 0;
      END;

      -- write the record to the log table
      INSERT INTO log_table
                  ( msg, client_info, module,
                    action, runmode, session_id, current_scn,
                    instance_name, machine, dbuser, osuser, code,
                    call_stack,
                    back_trace,
                    oper_id, logging_level
                  )
           VALUES ( l_msg, NVL( SELF.client_info, 'NA' ), NVL( SELF.module, 'NA' ),
                    NVL( SELF.action, 'NA' ), SELF.runmode, SELF.session_id, l_scn,
                    SELF.instance_name, SELF.machine, SELF.dbuser, SELF.osuser, l_code,
                    l_whence,
                    REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace, 1, 4000 ),
                                    '[[:cntrl:]]',
                                    '; '
                                  ),
                    p_oper_id, logging_level
                  );

      COMMIT;

           -- also output the message to the screen
           -- the client can control whether or not they want to see this
           -- in sqlplus, just SET SERVEROUTPUT ON or OFF
      -- by default, all messages are logged to STDOUT
      -- this can be controlled per message with P_STDOUT, which defaults to 'yes'
      IF REGEXP_LIKE( 'yes', p_stdout, 'i' )
      THEN
         DBMS_OUTPUT.put_line( p_msg );
      END IF;
   END log_msg;
   MEMBER PROCEDURE log_err
   AS
      l_msg   VARCHAR2( 1020 ) DEFAULT SQLERRM;
   BEGIN
      log_msg( l_msg, 1, 'no' );
   END log_err;
   MEMBER PROCEDURE log_cnt_msg(
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
           VALUES ( NVL( SELF.client_info, 'NA' ), NVL( SELF.module, 'NA' ),
                    NVL( SELF.action, 'NA' ), SELF.runmode, SELF.session_id, p_count
                  );

      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected' ) || ': ' || p_count,
               p_level,
               p_stdout,
               p_oper_id
             );
      COMMIT;
   END log_cnt_msg;
END;
/