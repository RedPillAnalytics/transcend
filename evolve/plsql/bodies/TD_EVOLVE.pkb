CREATE OR REPLACE PACKAGE BODY td_sql
AS

   -- checks things about a table depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_table(
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL
   )
   AS
      l_tab_name         VARCHAR2( 61 )     := UPPER( p_owner ) || '.'
                                               || UPPER( p_table );
      l_part_name        VARCHAR2( 92 )       := l_tab_name || ':' || UPPER( p_partname );
      l_partitioned      VARCHAR2( 3 );
      l_iot              VARCHAR2( 3 );
      l_compressed       VARCHAR2( 3 );
      l_partition_name   all_tab_partitions.partition_name%TYPE;
   BEGIN
      BEGIN
         SELECT CASE
                   WHEN compression = 'DISABLED'
                      THEN 'no'
                   WHEN compression = 'N/A'
                      THEN 'no'
                   WHEN compression IS NULL
                      THEN 'no'
                   ELSE 'yes'
                END,
                LOWER( partitioned ) partitioned,
                CASE iot_type
                   WHEN 'IOT'
                      THEN 'yes'
                   ELSE 'no'
                END iot
           INTO l_compressed,
                l_partitioned,
                l_iot
           FROM all_tables
          WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( td_inst.get_err_cd( 'no_tab' ),
                                     td_inst.get_err_msg( 'no_tab' ) || ': ' || l_tab_name
                                   );
      END;

      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         raise_application_error
                        ( td_inst.get_err_cd( 'parms_not_compatible' ),
                             td_inst.get_err_msg( 'parms_not_compatible' )
                          || ': '
                          || 'P_COMPRESSED requires P_PARTNAME when the table is partitioned'
                        );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_partitioned' ),
                                        td_inst.get_err_msg( 'not_partitioned' )
                                     || ': '
                                     || l_tab_name
                                   );
         END IF;

         BEGIN
            SELECT CASE
                      WHEN compression = 'DISABLED'
                         THEN 'no'
                      WHEN compression = 'N/A'
                         THEN 'no'
                      WHEN compression IS NULL
                         THEN 'no'
                      ELSE 'yes'
                   END
              INTO l_compressed
              FROM all_tab_partitions
             WHERE table_owner = UPPER( p_owner )
               AND table_name = UPPER( p_table )
               AND partition_name = UPPER( p_partname );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error( td_inst.get_err_cd( 'no_part' ),
                                           td_inst.get_err_msg( 'no_part' )
                                        || ': '
                                        || l_part_name
                                      );
         END;
      END IF;

      CASE
         WHEN td_ext.is_true( p_partitioned, TRUE )
              AND NOT td_ext.is_true( l_partitioned )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_partitioned' ),
                                        td_inst.get_err_msg( 'not_partitioned' )
                                     || ': '
                                     || l_tab_name
                                   );
         WHEN NOT td_ext.is_true( p_partitioned, TRUE )
              AND td_ext.is_true( l_partitioned )
         THEN
            raise_application_error( td_inst.get_err_cd( 'partitioned' ),
                                        td_inst.get_err_msg( 'partitioned' )
                                     || ': '
                                     || l_tab_name
                                   );
         WHEN td_ext.is_true( p_iot, TRUE ) AND NOT td_ext.is_true( l_iot )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_iot' ),
                                     td_inst.get_err_msg( 'not_iot' ) || ': '
                                     || l_tab_name
                                   );
         WHEN NOT td_ext.is_true( p_iot, TRUE ) AND td_ext.is_true( l_iot )
         THEN
            raise_application_error( td_inst.get_err_cd( 'iot' ),
                                     td_inst.get_err_msg( 'iot' ) || ': ' || l_tab_name
                                   );
         WHEN td_ext.is_true( p_compressed, TRUE ) AND NOT td_ext.is_true( l_compressed )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_compressed' ),
                                        td_inst.get_err_msg( 'not_compressed' )
                                     || ': '
                                     || CASE
                                           WHEN p_partname IS NULL
                                              THEN l_tab_name
                                           ELSE l_part_name
                                        END
                                   );
         WHEN NOT td_ext.is_true( p_compressed, TRUE ) AND td_ext.is_true( l_compressed )
         THEN
            raise_application_error( td_inst.get_err_cd( 'compressed' ),
                                        td_inst.get_err_msg( 'compressed' )
                                     || ': '
                                     || CASE
                                           WHEN p_partname IS NULL
                                              THEN l_tab_name
                                           ELSE l_part_name
                                        END
                                   );
         ELSE
            NULL;
      END CASE;
   END check_table;

   -- checks things about an object depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_object(
      p_owner         VARCHAR2,
      p_object        VARCHAR2,
      p_object_type   VARCHAR2 DEFAULT NULL
   )
   AS
      l_obj_name      VARCHAR2( 61 )       := UPPER( p_owner ) || '.'
                                              || UPPER( p_object );
      l_object_name   all_objects.object_name%TYPE;
   BEGIN
      BEGIN
         SELECT DISTINCT object_name
                    INTO l_object_name
                    FROM all_objects
                   WHERE owner = UPPER( p_owner )
                     AND object_name = UPPER( p_object )
                     AND REGEXP_LIKE( object_type, NVL( p_object_type, '.' ), 'i' );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( td_inst.get_err_cd( 'no_or_wrong_object' ),
                                        td_inst.get_err_msg( 'no_or_wrong_object' )
                                     || ': '
                                     || l_obj_name
                                   );
         WHEN TOO_MANY_ROWS
         THEN
            raise_application_error( td_inst.get_err_cd( 'too_many_objects' ),
                                     td_inst.get_err_msg( 'too_many_objects' )
                                   );
      END;
   END check_object;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path( p_dirname VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER( p_dirname );

      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error( -20010, 'Directory object does not exist' );
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
   BEGIN
      SELECT directory_name
        INTO l_dirname
        FROM all_directories
       WHERE directory_path = p_dir_path;

      RETURN l_dirname;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error( -20011,
                                  'No directory object defined for the specified path'
                                );
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error
                        ( -20012,
                          'More than one directory object defined for the specified path'
                        );
   END get_dir_name;

   -- returns a boolean
   -- does a check to see if a table exists
   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   dba_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM dba_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END table_exists;

   -- returns a boolean
   -- does a check to see if table is partitioned
   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_partitioned   dba_tables.partitioned%TYPE;
   BEGIN
      IF NOT table_exists( UPPER( p_owner ), UPPER( p_table ))
      THEN
         raise_application_error( td_inst.get_err_cd( 'no_tab' ),
                                     td_inst.get_err_msg( 'no_tab' )
                                  || ': '
                                  || p_owner
                                  || '.'
                                  || p_table
                                );
      END IF;

      SELECT partitioned
        INTO l_partitioned
        FROM dba_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      CASE
         WHEN td_ext.is_true( l_partitioned )
         THEN
            RETURN TRUE;
         WHEN NOT td_ext.is_true( l_partitioned )
         THEN
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_part_table;

   -- returns a boolean
   -- does a check to see if a object exists
   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_object   dba_objects.object_name%TYPE;
   BEGIN
      SELECT DISTINCT object_name
                 INTO l_object
                 FROM dba_objects
                WHERE owner = UPPER( p_owner ) AND object_name = UPPER( p_object );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END object_exists;

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
   FUNCTION exec_sql(
      p_sql              VARCHAR2,
      p_auto             VARCHAR2 DEFAULT 'no',
      p_msg              VARCHAR2 DEFAULT NULL,
      p_override_debug   VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   AS
      l_results   NUMBER;
   BEGIN

      IF NOT td_inst.is_debugmode OR NOT td_ext.is_true( p_override_debug )
      THEN
         IF td_ext.is_true( p_auto )
         THEN
            l_results := exec_auto( p_sql => p_sql );
         ELSE
            EXECUTE IMMEDIATE p_sql;

            l_results := SQL%ROWCOUNT;
         END IF;
      END IF;

      RETURN l_results;
   END exec_sql;

   -- if I don't care about the number of results (DDL, for instance), just call this procedure
   -- if P_AUTO is 'no' and P_CONCURRENT is 'no', then SQL%ROWCOUNT will still give the correct results after this call
   PROCEDURE exec_sql(
      p_sql              VARCHAR2,
      p_auto             VARCHAR2 DEFAULT 'no',
      p_msg              VARCHAR2 DEFAULT NULL,
      p_override_debug   VARCHAR2 DEFAULT 'no'
   )
   AS
      l_results   NUMBER;
   BEGIN
      -- simply call the procedure and discard the results
      l_results := exec_sql( p_sql => p_sql, p_auto => p_auto, p_msg => p_msg );
   END exec_sql;
   
   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE submit_sql(
      p_sql         VARCHAR2,
      p_msg         VARCHAR2 DEFAULT NULL,
      p_background  VARCHAR2 DEFAULT 'no',
      p_program	    VARCHAR2 DEFAULT 'consume_sql_job',
      p_job_class   VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS'
   )
   AS
      l_job_name all_scheduler_job_run_details.job_name%type := dbms_scheduler.generate_job_name(td_inst.module);
      l_module      VARCHAR2(32) := td_inst.module;
      l_action      VARCHAR2(24) := td_inst.action;
      l_session_id  NUMBER 	 := sys_context('USERENV','SESSIONID');
   BEGIN
      td_inst.log_msg('The job name is: '||l_job_name, 4);
      -- for now, we will always use the same program, CONSUME_SQL_JOB
      -- in the future, each module may have it's own program
      dbms_scheduler.create_job( l_job_name, program_name=>p_program, job_class=>p_job_class );
      
      -- define the values for each argument
      dbms_scheduler.set_job_argument_value( job_name          => l_job_name,
					     argument_position =>1,
					     argument_value    =>l_session_id);
      dbms_scheduler.set_job_argument_value( job_name          => l_job_name,
					     argument_position => 2,
					     argument_value    => l_module);
      dbms_scheduler.set_job_argument_value( job_name 	       => l_job_name,
					     argument_position => 3,
					     argument_value    => l_action);
      dbms_scheduler.set_job_argument_value( job_name	       => l_job_name,
					     argument_position => 4,
					     argument_value    => p_sql);
      dbms_scheduler.set_job_argument_value( job_name 	       => l_job_name,
					     argument_position => 5,
					     argument_value    => p_msg);

      -- enable the job
      dbms_scheduler.ENABLE(l_job_name);      
      -- run the job
      -- if p_session is affirmative, then execute within the same session
      -- if it's not, then schedule the job to be picked up by the scheduler
      dbms_scheduler.run_job(l_job_name, NOT td_ext.is_true( p_background ));
   END submit_sql;
   
   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE coordinate_sql(
      p_sleep    NUMBER DEFAULT 5,
      p_timeout	 NUMBER DEFAULT 0
   )
   AS
      l_sid     NUMBER;
      l_serial  NUMBER;
   BEGIN
      NULL;
   EXCEPTION
      WHEN others
      THEN 
      td_inst.log_err;
      RAISE;
   END coordinate_sql;   
   
   -- used to write a standard message to the LOG_TABLE
   PROCEDURE log_msg(
      p_msg      VARCHAR2,
      p_level    NUMBER DEFAULT 2
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
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
      IF g_logging_level >= p_level
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module, action, service_name, runmode,
                       session_id, current_scn, instance_name, machine, dbuser,
                       osuser, code, call_stack,
                       back_trace,
                       batch_id
                     )
              VALUES ( l_msg, g_client_info, g_module, g_action, g_service_name, g_runmode,
                       g_session_id, l_scn, g_instance_name, g_machine, g_dbuser,
                       g_osuser, 0, l_whence,
                       null,
                       g_batch_id
                     );

         COMMIT;

         -- also output the message to the screen
         -- the client can control whether or not they want to see this
         -- in sqlplus, just SET SERVEROUTPUT ON or OFF
         DBMS_OUTPUT.put_line( p_msg );
      END IF;
   END log_msg;   

   PROCEDURE log_cnt_msg(
      p_count     NUMBER,
      p_msg       VARCHAR2 DEFAULT NULL,
      p_level     NUMBER DEFAULT 2
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  ( client_info, module,
                    action, runmode, session_id, row_cnt
                  )
           VALUES ( g_client_info, g_module,
                    g_action, g_runmode, g_session_id, p_count
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
      l_code     NUMBER  := SQLCODE;
      l_msg      log_table.msg%TYPE := SQLERRM;
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
      IF g_logging_level >= 1
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     ( msg, client_info, module, action, service_name, runmode,
                       session_id, current_scn, instance_name, machine, dbuser,
                       osuser, code, call_stack,
                       back_trace,
                       batch_id
                     )
              VALUES ( l_msg, g_client_info, g_module, g_action, g_service_name, g_runmode,
                       g_session_id, l_scn, g_instance_name, g_machine, g_dbuser,
                       g_osuser, l_code, l_whence,
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

      END IF;
   END log_err;
   
   -- writes error information to the log_table
   PROCEDURE raise_err ( p_name VARCHAR2,
			 p_add_msg VARCHAR2 DEFAULT null )
   AS
   BEGIN
      raise_application_error( get_err_cd( p_name),
			       get_err_msg( p_name )||CASE WHEN p_add_msg IS NULL THEN NULL ELSE ': '||p_add_msg end
			     );
   END raise_err;
   
   -- return a Boolean determining runmode
   FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE
      WHEN REGEXP_LIKE(td_inst.runmode,'debug','i')
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

END td_sql;
/
SHOW errors