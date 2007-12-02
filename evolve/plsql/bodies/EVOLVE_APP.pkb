CREATE OR REPLACE PACKAGE BODY evolve_app
AS
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
      p_sql          VARCHAR2,
      p_msg          VARCHAR2 DEFAULT NULL,
      p_background   VARCHAR2 DEFAULT 'no',
      p_program      VARCHAR2 DEFAULT 'consume_sql_job',
      p_job_class    VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS'
   )
   AS
      l_job_name     all_scheduler_job_run_details.job_name%TYPE
                                    := DBMS_SCHEDULER.generate_job_name( td_inst.module );
      l_module       VARCHAR2( 32 )                                := td_inst.module;
      l_action       VARCHAR2( 24 )                                := td_inst.action;
      l_session_id   NUMBER                      := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   BEGIN
      evolve_log.log_msg( 'The job name is: ' || l_job_name, 4 );
      -- for now, we will always use the same program, CONSUME_SQL_JOB
      -- in the future, each module may have it's own program
      DBMS_SCHEDULER.create_job( l_job_name,
                                 program_name      => p_program,
                                 job_class         => p_job_class
                               );
      -- define the values for each argument
      DBMS_SCHEDULER.set_job_argument_value( job_name               => l_job_name,
                                             argument_position      => 1,
                                             argument_value         => l_session_id
                                           );
      DBMS_SCHEDULER.set_job_argument_value( job_name               => l_job_name,
                                             argument_position      => 2,
                                             argument_value         => l_module
                                           );
      DBMS_SCHEDULER.set_job_argument_value( job_name               => l_job_name,
                                             argument_position      => 3,
                                             argument_value         => l_action
                                           );
      DBMS_SCHEDULER.set_job_argument_value( job_name               => l_job_name,
                                             argument_position      => 4,
                                             argument_value         => p_sql
                                           );
      DBMS_SCHEDULER.set_job_argument_value( job_name               => l_job_name,
                                             argument_position      => 5,
                                             argument_value         => p_msg
                                           );
      -- enable the job
      DBMS_SCHEDULER.ENABLE( l_job_name );
      -- run the job
      -- if p_session is affirmative, then execute within the same session
      -- if it's not, then schedule the job to be picked up by the scheduler
      DBMS_SCHEDULER.run_job( l_job_name, NOT td_core.is_true( p_background ));
   END submit_sql;

   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE coordinate_sql( p_sleep NUMBER DEFAULT 5, p_timeout NUMBER DEFAULT 0 )
   AS
      l_sid      NUMBER;
      l_serial   NUMBER;
   BEGIN
      NULL;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END coordinate_sql;

   -- this process is called by submitted jobs to DBMS_SCHEDULER
   -- when SQL is submitted through SUBMIT_SQL, this is what those submitted jobs actually call
   PROCEDURE consume_sql(
      p_session_id   NUMBER,
      p_module       VARCHAR2,
      p_action       VARCHAR2,
      p_sql          VARCHAR2,
      p_msg          VARCHAR2
   )
   AS
   BEGIN
      -- use the SET_SCHEDULER_SESSION_ID procedure to register with the framework
      -- this allows all logging entries to be kept together
      td_inst.set_scheduler_info( p_session_id      => p_session_id,
                                  p_module          => p_module,
                                  p_action          => p_action
                                );

      -- load session parameters configured in PARAMETER_CONF for this module
      -- this is usually done by EVOLVE_OT, but that is not applicable here
      FOR c_params IN
         ( SELECT CASE
                     WHEN REGEXP_LIKE( NAME, 'enable|disable', 'i' )
                        THEN 'alter session ' || NAME || ' ' || VALUE
                     ELSE 'alter session set ' || NAME || '=' || VALUE
                  END DDL
            FROM parameter_conf
           WHERE LOWER( module ) = td_inst.module )
      LOOP
         IF evolve_log.is_debugmode
         THEN
            evolve_log.log_msg( 'Session SQL: ' || c_params.DDL );
         ELSE
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
      END LOOP;

      -- just use the standard procedure to execute the SQL
      exec_sql( p_sql => p_sql, p_msg => p_msg );
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END consume_sql;
END evolve_app;
/

SHOW errors