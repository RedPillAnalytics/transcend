CREATE OR REPLACE PACKAGE BODY evolve_app
AS
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
   FUNCTION exec_sql(
      p_sql         VARCHAR2,
      p_msg    	    VARCHAR2 DEFAULT NULL,
      p_auto   	    VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER
   AS
      l_results   NUMBER;
   BEGIN
      evolve_log.log_msg( 'P_AUTO: '||p_auto, 5 );
      evolve_log.log_msg( CASE
                          WHEN p_msg IS NULL
                             THEN 'SQL: ' || p_sql
                          ELSE p_msg
                       END, 3 );

      IF NOT evolve_log.is_debugmode
      THEN
         IF td_core.is_true( p_auto )
         THEN
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
      p_sql         VARCHAR2,
      p_msg    	    VARCHAR2 DEFAULT NULL,
      p_auto   	    VARCHAR2 DEFAULT 'no',
      p_background  VARCHAR2 DEFAULT 'no'
   )
   AS
      l_results   NUMBER;
   BEGIN
      evolve_log.log_msg( CASE
                          WHEN p_msg IS NULL
                             THEN 'SQL: ' || p_sql
                          ELSE p_msg
                       END, 3 );

      IF NOT evolve_log.is_debugmode
      THEN
	 evolve_log.log_msg('P_BACKGROUND: '||p_background,5);
	 evolve_log.log_msg('P_AUTO: '||p_auto,5);
	 CASE
	 WHEN td_core.is_true( p_background )
         THEN
	    submit_sql( p_sql => p_sql );
         WHEN td_core.is_true( p_auto )
         THEN
            l_results := exec_auto( p_sql => p_sql );
         ELSE
	    EXECUTE IMMEDIATE p_sql;
	 END CASE;

            l_results := SQL%ROWCOUNT;
      END IF;
   END exec_sql;

   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE submit_sql(
      p_sql         VARCHAR2,
      p_job_class   VARCHAR2 DEFAULT 'consume_sql_class'
   )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_job_name          all_scheduler_jobs.job_name%TYPE;
      l_module            VARCHAR2( 48 )                       := td_inst.module;
      l_action            VARCHAR2( 32 )                       := td_inst.action;
      l_session_id        NUMBER                 := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      l_job_action        all_scheduler_jobs.job_action%TYPE;
      l_client_id	  all_scheduler_jobs.client_id%TYPE := l_module||'-'||l_action||'-'||l_session_id;
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

      -- the CLIENT_ID will be available in the running jobs as well
      -- it will also be written to the scheduler logging metadata
      -- construct a CLIENT_ID as a combination of MODULE, ACTION, and SESSION_ID      
      evolve_log.log_msg('The CLIENT_ID is: '||l_client_id,4);
      DBMS_SESSION.set_identifier( l_client_id );

      -- generate the job action
      l_job_action :=
            'begin evolve_app.consume_sql('
         || l_session_id
         || ','''
         || l_module
         || ''','''
         || l_action
         || ''','''
         || p_sql
         || '''); end;';
      evolve_log.log_msg( 'The scheduler job action is: ' || l_job_action, 4 );

      -- schedule the job
      DBMS_SCHEDULER.create_job( l_job_name,
                                 job_class       => p_job_class,
                                 job_type        => 'plsql_block',
                                 job_action      => l_job_action
                               );
      
      dbms_scheduler.ENABLE( l_job_name );
      
      evolve_log.log_msg( 'Job ' || l_job_name || ' submitted to the Oracle scheduler' );
      
   END submit_sql;

   -- this process will execute through DBMS_SCHEDULER
   PROCEDURE coordinate_sql( p_sleep NUMBER DEFAULT 5, 
			     p_timeout NUMBER DEFAULT 0 )
   AS
      l_client_id     all_scheduler_jobs.client_id%TYPE;
      l_running       NUMBER;
      l_failed 	      NUMBER;
      l_start_secs    NUMBER := dbms_utility.get_time;
   BEGIN
      -- construct the CLIENT_ID
      l_client_id := td_inst.module||'-'||td_inst.action||'-'||td_inst.session_id;
      LOOP
	 -- the amount of time to wait
	 dbms_lock.sleep( p_sleep );
	 
	 -- get the count of running jobs
	 SELECT count(*) 
	   INTO l_running 
	   FROM all_scheduler_jobs
	  WHERE client_id = l_client_id
	    AND state = 'RUNNING';
	 
	 -- get the count of failed jobs
	 SELECT count(*) 
	   INTO l_failed
	   FROM all_scheduler_jobs
	  WHERE client_id = l_client_id
	    AND state = 'RUNNING';
	 
	 -- raise an error if there are failed jobs
	 IF l_failed > 0
	 THEN
	    evolve_log.raise_err( 'submit_sql' );
	 END IF;
	 
	 -- check for the timeout
	 IF (( dbms_utility.get_time - l_start_secs ) > p_timeout) AND p_timeout <> 0
	 THEN
	    evolve_log.raise_err( 'submit_sql_timeout' );
	 END IF;

	 EXIT WHEN l_running = 0;
      END LOOP;
   END coordinate_sql;

   -- this process is called by submitted jobs to DBMS_SCHEDULER
   -- when SQL is submitted through SUBMIT_SQL, this is what those submitted jobs actually call
   PROCEDURE consume_sql(
      p_session_id   NUMBER,
      p_module       VARCHAR2,
      p_action       VARCHAR2,
      p_sql          VARCHAR2
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
         evolve_log.log_err;
         RAISE;
   END consume_sql;
END evolve_app;
/

SHOW errors