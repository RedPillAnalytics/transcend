CREATE OR REPLACE PACKAGE BODY evolve_log
AS
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
   PROCEDURE log_msg( p_msg VARCHAR2, p_level NUMBER DEFAULT 2 )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_msg      log_table.msg%TYPE;
      l_scn      NUMBER	:= dbms_flashback.get_system_change_number;
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

      -- check to see the logging level to see if the message should be written
      IF td_inst.logging_level >= p_level
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module, action,
                       service_name, runmode, session_id, current_scn,
                       instance_name, machine, dbuser,
                       osuser, code, call_stack, back_trace, batch_id
                     )
              VALUES ( l_msg, td_inst.client_info, td_inst.module, td_inst.action,
                       td_inst.service_name, td_inst.runmode, td_inst.session_id, l_scn,
                       td_inst.instance_name, td_inst.machine, td_inst.dbuser,
                       td_inst.osuser, 0, l_whence, NULL, td_inst.batch_id
                     );

         COMMIT;
         -- also output the message to the screen
         -- the client can control whether or not they want to see this
         -- in sqlplus, just SET SERVEROUTPUT ON or OFF
         DBMS_OUTPUT.put_line( p_msg );
      END IF;
   END log_msg;

   PROCEDURE log_cnt_msg(
      p_count   NUMBER,
      p_msg     VARCHAR2 DEFAULT NULL,
      p_level   NUMBER DEFAULT 2
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  ( client_info, module, action,
                    runmode, session_id, row_cnt
                  )
           VALUES ( td_inst.client_info, td_inst.module, td_inst.action,
                    td_inst.runmode, td_inst.session_id, p_count
                  );

      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected' ) || ': ' || p_count,
               p_level
             );
      COMMIT;
   END log_cnt_msg;

   -- writes error information to the log_table
   PROCEDURE log_err
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_code     NUMBER               := SQLCODE;
      l_msg      log_table.msg%TYPE   := SQLERRM;
      l_scn      NUMBER;
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   BEGIN
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
      IF td_inst.logging_level >= 1
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module, action,
                       service_name, runmode, session_id, current_scn,
                       instance_name, machine, dbuser,
                       osuser, code, call_stack,
                       back_trace,
                       batch_id
                     )
              VALUES ( l_msg, td_inst.client_info, td_inst.module, td_inst.action,
                       td_inst.service_name, td_inst.runmode, td_inst.session_id, l_scn,
                       td_inst.instance_name, td_inst.machine, td_inst.dbuser,
                       td_inst.osuser, l_code, l_whence,
                       REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace,
                                               1,
                                               4000
                                             ),
                                       '[[:cntrl:]]',
                                       '; '
                                     ),
                       td_inst.batch_id
                     );

         COMMIT;
      END IF;
   END log_err;

   -- raises an error using RAISE_APPLICATION_ERROR
   -- uses a configuration table to find the error code and the message
   PROCEDURE raise_err( p_name VARCHAR2, p_add_msg VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      log_msg( 'The error name passed was '||p_name, 5);
      raise_application_error( td_inst.get_err_cd( p_name ),
                                  td_inst.get_err_msg( p_name )
                               || CASE
                                     WHEN p_add_msg IS NULL
                                        THEN NULL
                                     ELSE ': ' || p_add_msg
                                  END
                             );
   END raise_err;


   -- return a Boolean determining runmode
   FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE
         WHEN REGEXP_LIKE( td_inst.runmode, 'debug', 'i' )
            THEN TRUE
         ELSE FALSE
      END;
   END is_debugmode;

   -- begins debug mode
   PROCEDURE start_debug
   AS
   BEGIN
      td_inst.runmode( 'full debug' );
   END start_debug;

   -- stops debug mode
   PROCEDURE stop_debug
   AS
   BEGIN
      td_inst.runmode( 'runtime' );
   END stop_debug;
END evolve_log;
/

SHOW errors