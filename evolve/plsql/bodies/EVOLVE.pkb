CREATE OR REPLACE PACKAGE BODY evolve
AS
   -- get the currently configured action
   FUNCTION get_action
      RETURN VARCHAR2
   AS
      l_action   VARCHAR2( 32 );
   BEGIN
      RETURN td_inst.action;
   END get_action;
   
   -- get the currently configured module
   FUNCTION get_module
      RETURN VARCHAR2
   AS
      l_module   VARCHAR2( 32 );
   BEGIN
      RETURN td_inst.module;
   END get_module;
   
   -- get the system change number (SCN)
   -- there is a system procedure for this: DBMS_FLASHBACK.get_system_change_number
   -- but granting execute on this package also grants A LOT of other functionality
   -- the safer route seems to be to select the SCN from v$database
   -- this can be granted with a simple SELECT permission
   FUNCTION get_scn
      RETURN v$database.current_scn%type
   AS
      l_scn     v$database.current_scn%type;
   BEGIN

      SELECT current_scn
        INTO l_scn
        FROM v$database;
      
      RETURN l_scn;
      
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
         RAISE;

   END get_scn;

   -- used to pull the calling block from the dictionary
   -- used to populate CALL_STACK column in the LOG_TABLE
   FUNCTION whence
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
   PROCEDURE log_msg( p_msg VARCHAR2, p_level NUMBER DEFAULT 1 )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_msg      log_table.msg%TYPE;
      l_scn      NUMBER               := get_scn;
      l_schema   VARCHAR2( 30 );
      l_entry_ts TIMESTAMP;
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   BEGIN
      -- still write as much to the logfile if we can even if it's too large for the log table
      BEGIN
         l_msg := p_msg;
      EXCEPTION
         WHEN VALUE_ERROR
         THEN
            l_msg := SUBSTR( p_msg, 0, 1998 ) || '>>';
      END;

      -- find out what called me
      l_whence := whence;
      
      -- get the timestamp for consistency
      l_entry_ts := systimestamp;

      -- check to see the logging level to see if the message should be written
      IF td_inst.logging_level >= p_level
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( entry_ts, msg, client_info, module, action, service_name,
                       runmode, session_id, current_scn, instance_name, machine,
                       dbuser, osuser, code, call_stack, back_trace, batch_id, logging_level
                     )
              VALUES ( l_entry_ts, l_msg, td_inst.client_info, td_inst.module, td_inst.action, td_inst.service_name,
                       td_inst.runmode, td_inst.session_id, l_scn, td_inst.instance_name, td_inst.machine,
                       td_inst.dbuser, td_inst.osuser, 0, l_whence, NULL, td_inst.batch_id, p_level
                     );

         COMMIT;
         -- also output the message to the screen
         -- the client can control whether or not they want to see this
         -- in sqlplus, just SET SERVEROUTPUT ON or OFF
         DBMS_OUTPUT.put_line( p_msg );
      END IF;
   END log_msg;

   PROCEDURE log_results_msg( 
      p_count       NUMBER,
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_category    VARCHAR2,
      p_msg         VARCHAR2 DEFAULT NULL, 
      p_level       NUMBER   DEFAULT 1
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO results_table
             ( client_info, module, action, runmode, session_id, object_owner, object_name, dml_category, row_count, duration
                  )
             VALUES ( td_inst.client_info, td_inst.module, td_inst.action, td_inst.runmode, td_inst.session_id, upper(p_owner), upper(p_object), p_category, p_count, td_inst.get_elapsed_time
                  );

      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected' ) || ': ' || p_count, p_level );
      COMMIT;
   END log_results_msg;

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
         WHEN others
         THEN
            l_scn := 0;
      END;

      -- check to see the logging level to see if the message should be written
      IF td_inst.logging_level >= 1
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module, action, service_name,
                       runmode, session_id, current_scn, instance_name, machine,
                       dbuser, osuser, code, call_stack,
                       back_trace, batch_id, logging_level
                     )
              VALUES ( l_msg, td_inst.client_info, td_inst.module, td_inst.action, td_inst.service_name,
                       td_inst.runmode, td_inst.session_id, l_scn, td_inst.instance_name, td_inst.machine,
                       td_inst.dbuser, td_inst.osuser, l_code, l_whence,
                       REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace, 1, 4000 ), '[[:cntrl:]]', '; ' ),
                       td_inst.batch_id, 1
                     );

         COMMIT;
      END IF;
   END log_err;

   PROCEDURE log_variable( 
      p_name       VARCHAR2,
      p_value      VARCHAR2
   )
   AS
   BEGIN
      log_msg('The value of variable "'||upper(p_name)||'" is: '||p_value, 5);
   END log_variable;

   PROCEDURE log_variable( 
      p_name       VARCHAR2,
      p_value      NUMBER
   )
   AS
   BEGIN
      log_variable( p_name, to_char( p_value) );
   END log_variable;

   PROCEDURE log_variable( 
      p_name       VARCHAR2,
      p_value      DATE
   )
   AS
   BEGIN
      log_variable( p_name, to_char( p_value) );
   END log_variable;

   PROCEDURE log_variable( 
      p_name       VARCHAR2,
      p_value      BOOLEAN
   )
   AS
   BEGIN
      log_variable( p_name, CASE WHEN p_value THEN 'TRUE' ELSE 'FALSE' END );
   END log_variable;
   
   PROCEDURE log_exception( 
      p_name       VARCHAR2
   )
   AS
   BEGIN
      log_msg('Exception "'||upper(p_name)||'" was handled', 5);
   END log_exception;
   
      -- used to return a distinct error message number by label
   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER
   AS
      l_code   error_conf.code%TYPE;
   BEGIN
      BEGIN
         SELECT (0 - code)
           INTO l_code
           FROM error_conf
          WHERE NAME = p_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( -20001, 'The specified error has not been configured: '||p_name );
      END;

      RETURN l_code;
   END get_err_cd;
   
   -- used to return a distinct error message text string by label
   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_msg   error_conf.MESSAGE%TYPE;
   BEGIN
      BEGIN
         SELECT MESSAGE
           INTO l_msg
           FROM error_conf
          WHERE NAME = p_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( -20001, 'The specified error has not been configured: '||p_name );
      END;

      RETURN l_msg;
   END get_err_msg;

   -- raises an error using RAISE_APPLICATION_ERROR
   -- uses a configuration table to find the error code and the message
   PROCEDURE raise_err( p_name VARCHAR2, p_add_msg VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      log_msg( 'The error name passed: "' || p_name || '"', 5 );
      IF is_debugmode
      THEN
         log_msg( 'Error Raised: "'||get_err_cd( p_name )||': '||get_err_msg( p_name )||'"');
      ELSE         
         raise_application_error( get_err_cd( p_name ),
                                  get_err_msg( p_name ) || CASE
                                                           WHEN p_add_msg IS NULL
                                                           THEN NULL
                                                           ELSE ': ' || p_add_msg
                                                           END
                                );
      END IF;

   END raise_err;
   
   PROCEDURE print_query( p_query IN VARCHAR2 )
   IS
      l_thecursor     INTEGER           DEFAULT DBMS_SQL.open_cursor;
      l_columnvalue   VARCHAR2( 4000 );
      l_status        INTEGER;
      l_desctbl       DBMS_SQL.desc_tab;
      l_colcnt        NUMBER;
      l_cs            VARCHAR2( 255 );
      l_date_fmt      VARCHAR2( 255 );

      -- small inline procedure to restore the sessions state
      -- we may have modified the cursor sharing and nls date format
      -- session variables, this just restores them
      PROCEDURE restore
      IS
      BEGIN
         IF ( UPPER( l_cs ) NOT IN( 'FORCE', 'SIMILAR' ))
         THEN
            EXECUTE IMMEDIATE 'alter session set cursor_sharing=exact';
         END IF;

         DBMS_SQL.close_cursor( l_thecursor );
      END restore;
   BEGIN
      log_msg( 'Results printed below for:' || CHR( 10 ) || p_query || CHR( 10 ) || '-----------------', 4 );

      -- to be bind variable friendly on this ad-hoc queries, we
      -- look to see if cursor sharing is already set to FORCE or
      -- similar, if not, set it so when we parse -- literals
      -- are replaced with binds
      IF ( DBMS_UTILITY.get_parameter_value( 'cursor_sharing', l_status, l_cs ) = 1 )
      THEN
         IF ( UPPER( l_cs ) NOT IN( 'FORCE', 'SIMILAR' ))
         THEN
            EXECUTE IMMEDIATE 'alter session set cursor_sharing=force';
         END IF;
      END IF;

      -- parse and describe the query sent to us.  we need
      -- to know the number of columns and their names.
      DBMS_SQL.parse( l_thecursor, p_query, DBMS_SQL.native );
      DBMS_SQL.describe_columns( l_thecursor, l_colcnt, l_desctbl );

      -- define all columns to be cast to varchar2's, we
      -- are just printing them out
      FOR i IN 1 .. l_colcnt
      LOOP
         IF ( l_desctbl( i ).col_type NOT IN( 113 ))
         THEN
            DBMS_SQL.define_column( l_thecursor, i, l_columnvalue, 4000 );
         END IF;
      END LOOP;

      -- execute the query, so we can fetch
      l_status := DBMS_SQL.EXECUTE( l_thecursor );

      -- loop and print out each column on a separate line
      -- bear in mind that dbms_output only prints 255 characters/line
      -- so we'll only see the first 200 characters by my design...
      WHILE( DBMS_SQL.fetch_rows( l_thecursor ) > 0 )
      LOOP
         FOR i IN 1 .. l_colcnt
         LOOP
            IF ( l_desctbl( i ).col_type NOT IN( 113 ))
            THEN
               DBMS_SQL.COLUMN_VALUE( l_thecursor, i, l_columnvalue );
               log_msg( RPAD( l_desctbl( i ).col_name, 30 ) || ': ' || SUBSTR( l_columnvalue, 1, 200 ), 4 );
            END IF;
         END LOOP;

         log_msg( '-----------------', 4 );
      END LOOP;

      -- now, restore the session state, no matter what
      restore;
   EXCEPTION
      WHEN OTHERS
      THEN
         restore;
         RAISE;
   END print_query;

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
   
   -- called by the EXEC_SQL function when an autonomous transaction is desired
   -- and the number of results are desired
   FUNCTION exec_auto( p_sql VARCHAR2 )
      RETURN NUMBER
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_results   NUMBER;
   BEGIN
      EXECUTE IMMEDIATE p_sql;

      l_results := SQL%ROWCOUNT;
      COMMIT;
      RETURN l_results;
   END exec_auto;

   -- accepts the P_AUTO flag and determines whether to execute the statement
   -- if the P_AUTO flag of 'yes' is passed, then EXEC_AUTO is called
   -- if P_BACKGROUND of 'yes' is called, then it is executed through DBMS_SCHEDULER
   FUNCTION exec_sql( p_sql VARCHAR2, p_msg VARCHAR2 DEFAULT NULL, p_auto VARCHAR2 DEFAULT 'no' )
      RETURN NUMBER
   AS
      l_results   NUMBER;
   BEGIN
      log_msg( 'This is an AUTONOMOUS_TRANSACTION', 5 );
      log_msg( CASE
                             WHEN p_msg IS NULL
                                THEN 'SQL: ' || p_sql
                             ELSE p_msg
                          END, 3 );

      IF NOT is_debugmode
      THEN
         IF td_core.is_true( p_auto )
         THEN
            
            log_msg( 'AUTONOMOUS_TRANSACTION initiated', 5 );

            l_results := exec_auto( p_sql => p_sql );
         ELSE
            EXECUTE IMMEDIATE p_sql;
         END IF;

         l_results := SQL%ROWCOUNT;
      END IF;

      RETURN l_results;
   END exec_sql;

   -- if I don't care about the number of results (DDL, for instance), just call this procedure
   -- accepts the P_AUTO flag and determines whether to execute the statement autonomously
   -- if the P_AUTO flag of 'yes' is passed, then EXEC_AUTO is called
   -- if P_BACKGROUND of 'yes' is called, then it is executed through SUBMIT_SQL
   PROCEDURE exec_sql(
      p_sql             VARCHAR2,
      p_msg             VARCHAR2 DEFAULT NULL,
      p_auto            VARCHAR2 DEFAULT 'no',
      p_concurrent_id   VARCHAR2 DEFAULT NULL
   )
   AS
      l_results   NUMBER;
   BEGIN

      log_msg( CASE
                             WHEN p_msg IS NULL
                                THEN 'SQL: ' || p_sql
                             ELSE p_msg || ': ' || p_sql
                          END, 3 );

      IF NOT is_debugmode
      THEN

         CASE
            WHEN p_concurrent_id IS NOT NULL
            THEN
               log_msg( 'The concurrent id is: ' || p_concurrent_id, 5 );
               submit_sql( p_sql => p_sql, p_concurrent_id => p_concurrent_id );
            WHEN td_core.is_true( p_auto )
            THEN

               log_msg( 'AUTONOMOUS_TRANSACTION initiated', 5 );

               l_results := exec_auto( p_sql => p_sql );

            ELSE
               EXECUTE IMMEDIATE p_sql;
         END CASE;

         l_results := SQL%ROWCOUNT;
      END IF;
   END exec_sql;

   -- uses a sequence to generate a unique concurrent id for concurrent processes
   -- this id is set with DBMS_SESSION.SET_IDENTIFIER
   FUNCTION get_concurrent_id
      RETURN VARCHAR2
   AS
      l_seq_value     NUMBER;
      l_concurrent_id VARCHAR2(100);
   BEGIN
      -- select the sequence nextval
      SELECT concurrent_id_seq.NEXTVAL
        INTO l_seq_value
        FROM DUAL;
      
      -- print the concurrent id to the log
      log_msg( 'The value from the sequence: ' || l_seq_value, 5 );
      
      l_concurrent_id := to_char(sys_context('USERENV','SESSIONID')||'-'||l_seq_value);
      -- print the concurrent id to the log
      log_msg( 'The generated concurrent_id is: ' || l_concurrent_id, 5 );
      RETURN l_concurrent_id;
   END get_concurrent_id;

   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE submit_sql( p_sql VARCHAR2, p_concurrent_id VARCHAR2, p_job_class VARCHAR2 DEFAULT 'EVOLVE_DEFAULT_CLASS' )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_job_name          all_scheduler_jobs.job_name%TYPE;
      l_module            VARCHAR2( 48 )                       := td_inst.module;
      l_action            VARCHAR2( 32 )                       := td_inst.action;
      l_session_id        NUMBER                               := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      l_job_action        all_scheduler_jobs.job_action%TYPE;
      e_invalid_jobname   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_invalid_jobname, -23481 );
   BEGIN
      BEGIN
         l_job_name := DBMS_SCHEDULER.generate_job_name( td_inst.module );
      EXCEPTION
         WHEN e_invalid_jobname
         THEN
            l_job_name := DBMS_SCHEDULER.generate_job_name;
      END;
      
      -- record the job name
      log_msg( 'The job name is: ' || l_job_name, 5 );

      -- use the unique concurrent id
      log_msg( 'The child concurrent_id is: ' || p_concurrent_id, 5 );
      DBMS_SESSION.set_identifier( p_concurrent_id );
      -- generate the job action
      l_job_action :=
            'begin evolve.consume_sql('
         || l_session_id
         || ','''
         || l_module
         || ''','''
         || l_action
         || ''','''
         || p_sql
         || '''); end;';
      log_msg( 'The scheduler job action is: ' || l_job_action, 4 );
      -- schedule the job
      DBMS_SCHEDULER.create_job( l_job_name,
                                 job_class       => p_job_class,
                                 job_type        => 'plsql_block',
                                 job_action      => l_job_action
                               );
      log_msg( 'Oracle Scheduler job ' || l_job_name || ' created', 4 );
      DBMS_SCHEDULER.ENABLE( l_job_name );
      log_msg( 'Oracle Scheduler job ' || l_job_name || ' enabled', 4 );
   END submit_sql;

   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE coordinate_sql(
      p_concurrent_id   VARCHAR2,
      p_raise_err       VARCHAR2 DEFAULT 'yes',
      p_sleep           NUMBER DEFAULT 5,
      p_timeout         NUMBER DEFAULT 0
   )
   AS
      l_running      NUMBER;
      l_failed       NUMBER;
      l_start_secs   NUMBER := DBMS_UTILITY.get_time;
   BEGIN
      LOOP
         -- the amount of time to wait
         DBMS_LOCK.sleep( p_sleep );

         -- get the count of running jobs
         SELECT COUNT( * )
           INTO l_running
           FROM all_scheduler_jobs
          WHERE client_id = p_concurrent_id AND state = 'RUNNING';

         -- get the count of failed jobs
         SELECT COUNT( * )
           INTO l_failed
           FROM ( SELECT DISTINCT MIN( operation ) OVER( PARTITION BY job_name ) operation,
                                  MIN( status ) OVER( PARTITION BY job_name ) status,
                                  MAX( client_id ) OVER( PARTITION BY job_name ) client_id, job_name
                            FROM all_scheduler_job_log )
          WHERE client_id = p_concurrent_id AND status = 'FAILED';

         log_msg( 'Failed job count: ' || l_failed, 5 );

         -- raise an error if there are failed jobs, but only if desired
         CASE
            WHEN td_core.is_true( p_raise_err ) AND l_failed > 0
            THEN
               raise_err( 'submit_sql' );
            WHEN NOT td_core.is_true( p_raise_err ) AND l_failed > 0
            THEN
               log_msg
                  ( 'Errors were generated by a process submitted to the Oracle Scheduler. See the scheduler logs for details.',
                    3
                  );
            ELSE
               NULL;
         END CASE;

         -- check for the timeout
         IF ( ( DBMS_UTILITY.get_time - l_start_secs ) > p_timeout ) AND p_timeout <> 0
         THEN
            raise_err( 'submit_sql_timeout' );
         END IF;

         EXIT WHEN l_running = 0;
      END LOOP;
   END coordinate_sql;

   -- this process is called by submitted jobs to DBMS_SCHEDULER
   -- when SQL is submitted through SUBMIT_SQL, this is what those submitted jobs actually call
   PROCEDURE consume_sql( p_session_id NUMBER, p_module VARCHAR2, p_action VARCHAR2, p_sql VARCHAR2 )
   AS
   BEGIN
      -- use the SET_SCHEDULER_SESSION_ID procedure to register with the framework
      -- this allows all logging entries to be kept together
      td_inst.set_scheduler_info( p_session_id => p_session_id, p_module => p_module, p_action => p_action );

      -- load session parameters configured in PARAMETER_CONF for this module
      -- this is usually done by EVOLVE_OT, but that is not applicable here
      FOR c_params IN ( SELECT CASE
                                  WHEN REGEXP_LIKE( NAME, 'enable|disable', 'i' )
                                     THEN 'alter session ' || NAME || ' ' || VALUE
                                  ELSE 'alter session set ' || NAME || '=' || VALUE
                               END DDL
                         FROM parameter_conf
                        WHERE LOWER( module ) = td_inst.module )
      LOOP
         -- use the standard execute immediate instead of the EXEC_SQL api
         -- there is no concept of DEBUG mode inside scheduler jobs, so don't complicate it
         EXECUTE IMMEDIATE c_params.DDL;
      END LOOP;

      -- use the standard execute immediate instead of the EXEC_SQL api
      -- there is no concept of DEBUG mode inside scheduler jobs, so don't complicate it
      EXECUTE IMMEDIATE p_sql;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
         RAISE;
   END consume_sql;

   -- this procedure will create an extract file with the log from the current session   
   PROCEDURE dump_log( p_directory VARCHAR2, p_repository VARCHAR2, p_dump_type VARCHAR2 DEFAULT 'session' )
   AS  
      l_object   all_objects.object_name%type;
      l_numrows  NUMBER;
      l_filename VARCHAR2(50);
   BEGIN
      
      -- determine which view to use
      l_object :=
      CASE p_dump_type
      WHEN 'session' THEN 'log_my_session'
      WHEN 'day' THEN 'log_today'
      WHEN 'week' THEN 'log_week'
      END; 
         
      l_filename :=  'evolve_' || sys_context('USERENV','SESSIONID') || '.dmp';

      -- extract data to the file
      l_numrows := td_utils.extract_object( p_owner     => p_repository,
                                            p_object    => l_object,
                                            p_directory => p_directory,
                                            p_filename  => l_filename,
                                            p_append    => 'yes' );

      evolve.log_msg( 'Dump file ' || p_directory || ':' || l_filename || ' created' );
                                            
      
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
         RAISE;
   END dump_log;

END evolve;
/

SHOW errors