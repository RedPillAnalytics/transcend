CREATE OR REPLACE TYPE BODY apptype
AS
   CONSTRUCTOR FUNCTION apptype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT
   AS
      l_results   NUMBER;
   BEGIN
      -- get the session id
      session_id := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      -- first we need to populate the module attribute, because it helps us determine parameter values
      module :=
         LOWER( CASE
                   WHEN p_module IS NULL
                      THEN get_package_name
                   ELSE get_package_name || '.' || p_module
                END
              );
      -- we also set the action, which may be used one day to fine tune parameters
      action := LOWER( p_action );
      -- set the runmode
      SELF.set_runmode( p_runmode );
      -- read previous app_info settings
      -- if not registering with oracle, then this is not necessary
      DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
      DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      -- populate attributes with new app_info settings
      client_info := NVL( p_client_info, prev_client_info );
      -- set other attributes
      instance_name := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
      dbuser := SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
      osuser := SYS_CONTEXT( 'USERENV', 'OS_USER' );
      machine :=
            SYS_CONTEXT( 'USERENV', 'HOST' )
         || '['
         || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         || ']';
      -- now set the new values
      DBMS_APPLICATION_INFO.set_client_info( client_info );
      DBMS_APPLICATION_INFO.set_module( module, action );
      RETURN;
   END apptype;
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
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2
   AS
      l_call_stack    VARCHAR2( 4096 ) DEFAULT DBMS_UTILITY.format_call_stack;
      l_num           NUMBER;
      l_found_stack   BOOLEAN          DEFAULT FALSE;
      l_line          VARCHAR2( 255 );
      l_cnt           NUMBER           := 0;
      l_name          VARCHAR2( 30 );
      l_caller        VARCHAR2( 30 );
   BEGIN
      LOOP
         l_num := INSTR( l_call_stack, CHR( 10 ));
         EXIT WHEN( l_cnt = 3 OR l_num IS NULL OR l_num = 0 );
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

            -- l_cnt = 1 is ME
            -- l_cnt = 2 is MY Caller
            -- l_cnt = 3 is Their Caller
            IF ( l_cnt = 3 )
            THEN
               l_line := SUBSTR( l_line, 21 );

               IF ( l_line LIKE 'pr%' )
               THEN
                  l_num := LENGTH( 'procedure ' );
               ELSIF( l_line LIKE 'fun%' )
               THEN
                  l_num := LENGTH( 'function ' );
               ELSIF( l_line LIKE 'package body%' )
               THEN
                  l_num := LENGTH( 'package body ' );
               ELSIF( l_line LIKE 'pack%' )
               THEN
                  l_num := LENGTH( 'package ' );
               ELSIF( l_line LIKE 'anonymous%' )
               THEN
                  l_num := LENGTH( 'anonymous block ' );
               ELSE
                  l_num := NULL;
               END IF;

               IF ( l_num IS NOT NULL )
               THEN
                  l_caller := LTRIM( RTRIM( UPPER( SUBSTR( l_line, 1, l_num - 1 ))));
               ELSE
                  l_caller := 'TRIGGER';
               END IF;

               l_line := SUBSTR( l_line, NVL( l_num, 1 ));
               l_num := INSTR( l_line, '.' );
               l_name := LTRIM( RTRIM( SUBSTR( l_line, l_num + 1 )));
            END IF;
         END IF;
      END LOOP;

      RETURN LOWER( l_name );
   END get_package_name;
   MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      action := LOWER( p_action );
      DBMS_APPLICATION_INFO.set_action( action );
   END change_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      action := prev_action;
      module := prev_module;
      client_info := prev_client_info;
      DBMS_APPLICATION_INFO.set_client_info( prev_client_info );
      DBMS_APPLICATION_INFO.set_module( prev_module, prev_action );
   END clear_app_info;
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

      INSERT INTO log_table
                  ( msg, client_info, module,
                    action, runmode, session_id, current_scn,
                    instance_name, machine, dbuser, osuser, code,
                    call_stack,
                    back_trace,
                    oper_id
                  )
           VALUES ( l_msg, NVL( SELF.client_info, 'NA' ), NVL( SELF.module, 'NA' ),
                    NVL( SELF.action, 'NA' ), SELF.runmode, SELF.session_id, l_scn,
                    SELF.instance_name, SELF.machine, SELF.dbuser, SELF.osuser, l_code,
                    l_whence,
                    REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace, 1, 4000 ),
                                    '[[:cntrl:]]',
                                    '; '
                                  ),
                    p_oper_id
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