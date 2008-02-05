CREATE OR REPLACE PACKAGE BODY td_install
IS
   g_user	    dba_users.username%TYPE;
   g_tablespace     dba_users.default_tablespace%TYPE;
   g_current_schema dba_users.username%TYPE;

   PROCEDURE create_user(
      p_user        VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT NULL
   ) 
   IS
      e_user_exists EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
      e_no_tbspace	 EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
   BEGIN
      BEGIN
	 EXECUTE IMMEDIATE 'CREATE USER '||p_user||' identified by no2'||p_user
	                 ||CASE 
			     WHEN p_tablespace IS NULL 
			     THEN 
			        NULL 
			     ELSE ' default tablespace '||p_tablespace 
			   END;
      EXCEPTION
	 WHEN e_user_exists
	 THEN
	    IF p_tablespace IS NOT NULL
	    THEN

	       g_user := p_user;
	       -- get the current default tablespace of the repository user
	       SELECT default_tablespace
		 INTO g_tablespace
		 FROM dba_users
		WHERE username=upper(p_user);
	       
	       EXECUTE IMMEDIATE 'alter user '||p_user||' default tablespace '||p_tablespace;
	    ELSE
	       NULL;
	    END IF;

	 WHEN e_no_tbspace
	 THEN
   	   raise_application_error(-20001,'Tablespace '||p_tablespace||' does not exist');
      END;
      
      IF p_tablespace IS NOT NULL
      THEN
	 -- gieve the user a quote
	 EXECUTE IMMEDIATE 'ALTER USER '||p_user||' QUOTA 50M ON '||p_tablespace;
      END IF;
      
   END create_user;
   
   PROCEDURE set_current_schema(
      p_schema    VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      l_current_schema	dba_users.username%TYPE;
      e_no_user EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_user, -1435 );
   BEGIN
      BEGIN
	 -- get the current schema before this
	 SELECT sys_context('USERENV','CURRENT_SCHEMA')
	   INTO l_current_schema
	   FROM dual;
      
      -- set the session to that user
	 EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||p_schema;
	 g_current_schema := l_current_schema;
      EXCEPTION
	 WHEN e_no_user
	 THEN raise_application_error(-20008, 'User "'||upper(p_schema)||'" does not exist.');
      END; 
   END set_current_schema;
   

   PROCEDURE reset_default_tablespace
   IS
   BEGIN
      IF g_tablespace IS NOT NULL AND g_user IS NOT null
      THEN
	 EXECUTE IMMEDIATE 'alter user '||g_user||' default tablespace '||g_tablespace;
	 g_tablespace := NULL;
	 g_user := NULL;
      END IF;
   END reset_default_tablespace;
   
   PROCEDURE reset_current_schema
   IS
   BEGIN
      EXECUTE IMMEDIATE 'alter session set current_schema='||g_current_schema;
   END reset_current_schema;

   -- this creates the job metadata (called a program) for submitting concurrent processes
   PROCEDURE create_scheduler_metadata
   IS
      e_no_sched_obj EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_sched_obj, -27476 );
   BEGIN
      
      -- first, drop the job class and the program
      BEGIN
	 dbms_scheduler.drop_job_class( job_class_name  => 'EVOLVE_DEFAULT_CLASS' );
      EXCEPTION
	 WHEN e_no_sched_obj
	 THEN
	 NULL;
      END;
      
      dbms_scheduler.create_job_class( job_class_name    => 'EVOLVE_DEFAULT_CLASS',
				       logging_level	 => DBMS_SCHEDULER.LOGGING_FULL,
				       comments		 =>   'Job class for the Evolve product by Transcendent Data, Inc.'
				       ||' This is the job class used by default when the Oracle scheduler is used for concurrent processing');

   END create_scheduler_metadata;


   PROCEDURE grant_evolve_rep_privs(
      p_schema   VARCHAR2 DEFAULT NULL,
      p_user     VARCHAR2 DEFAULT NULL,
      p_drop     BOOLEAN  DEFAULT FALSE    
   ) 
   IS
      l_sel_grant VARCHAR2(30);
      l_adm_grant VARCHAR2(30);
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
      e_role_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
      e_no_grantee   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grantee, -1919 );
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
   BEGIN
      CASE
      WHEN p_schema IS NOT NULL AND p_user IS NOT NULL
      THEN
      raise_application_error(-20006, 'Parameters P_SCHEMA and P_USER are mutually exclusive');
      WHEN p_user IS NOT NULL AND p_drop
      THEN
      raise_application_error(-20007, 'Specifying P_USER with a value of TRUE for P_DROP is not compatible');
      WHEN p_schema IS NOT NULL
      THEN
      l_sel_grant := p_schema||'_sel';
      l_adm_grant := p_schema||'_adm';
      WHEN p_user IS NOT NULL
      THEN
      l_sel_grant := p_user;
      l_adm_grant := p_user;
      ELSE
      NULL;
      END CASE;
      
      -- this will drop the roles before beginning
      IF p_drop AND p_schema IS NOT null
      THEN
	 BEGIN
	    EXECUTE IMMEDIATE 'DROP role '||l_sel_grant;
	 EXCEPTION
	    WHEN e_no_grantee
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE 'DROP role '||l_adm_grant;
	 EXCEPTION
	    WHEN e_no_grantee
	    THEN
	    NULL;
	 END;
	 
      END IF;

	 BEGIN
	    EXECUTE IMMEDIATE 'CREATE ROLE '||l_sel_grant;
	 EXCEPTION
	    WHEN e_role_exists
	    THEN
	      NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'CREATE ROLE '||l_adm_grant;
	 EXCEPTION
	    WHEN e_role_exists
	    THEN
	      NULL;
	 END;
	 
	 BEGIN
	    
	    -- first, the TDSYS tables
	    EXECUTE IMMEDIATE 'GRANT SELECT ON TDSYS.REPOSITORIES TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TDSYS.REPOSITORIES TO '||l_adm_grant;
	    
	    EXECUTE IMMEDIATE 'GRANT SELECT ON TDSYS.APPLICATIONS TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TDSYS.APPLICATIONS TO '||l_adm_grant;
	    
	    EXECUTE IMMEDIATE 'GRANT SELECT ON TDSYS.USERS TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TDSYS.USERS TO '||l_adm_grant;
	    
	    -- now the evolve repository tables
	    EXECUTE IMMEDIATE 'GRANT SELECT ON COUNT_TABLE TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON COUNT_TABLE TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON DIR_LIST TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON DIR_LIST TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON LOGGING_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON LOGGING_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON LOG_TABLE TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON LOG_TABLE TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON NOTIFICATION_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON NOTIFICATION_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON NOTIFICATION_EVENTS TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON NOTIFICATION_EVENTS TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON PARAMETER_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON PARAMETER_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON REGISTRATION_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON REGISTRATION_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON RUNMODE_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON RUNMODE_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON ERROR_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON ERROR_CONF TO '||l_adm_grant;
	    	
      EXCEPTION
	 WHEN e_no_grantee
	 THEN
	    raise_application_error(-20005,'The grantees '||l_sel_grant||' and '||l_adm_grant||' do not exist.');
	 WHEN e_no_obj
	    THEN
	    raise_application_error(-20004,'Some repository objects do not exist.');
      END;

   END grant_evolve_rep_privs;
  
   PROCEDURE grant_evolve_app_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      l_app_role VARCHAR2(30) := p_schema||'_app';
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
      e_role_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
      e_no_grantee   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grantee, -1919 );
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
      e_no_role   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
   BEGIN
      
      BEGIN
	 EXECUTE IMMEDIATE 'DROP role '||l_app_role;
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      
      BEGIN
	 EXECUTE IMMEDIATE 'CREATE ROLE '||l_app_role;
      EXCEPTION
	 WHEN e_role_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 -- types
	 EXECUTE IMMEDIATE 'grant execute on APP_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on EVOLVE_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on NOTIFICATION_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on SPLIT_OT to '||l_app_role;
	 -- packages
	 EXECUTE IMMEDIATE 'grant execute on STRAGG to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TD_CORE to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TD_INST to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on EVOLVE_LOG to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TD_UTILS to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on EVOLVE_APP to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on EVOLVE_ADM to '||l_app_role;
	 -- sequences
	 EXECUTE IMMEDIATE 'grant select on CONCURRENT_ID_SEQ to '||l_app_role;
	 
      EXCEPTION
	 WHEN e_no_obj
	 THEN
	 raise_application_error(-20004,'Some application objects do not exist.');
      END;

   END grant_evolve_app_privs;
  
   PROCEDURE grant_transcend_rep_privs(
      p_schema   VARCHAR2 DEFAULT NULL,
      p_user	 varchar2 DEFAULT NULL  
   ) 
   IS
      l_sel_grant VARCHAR2(30);
      l_adm_grant VARCHAR2(30);
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
      e_no_grantee	 EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grantee, -1917 );
   BEGIN
      CASE
      WHEN p_schema IS NOT NULL AND p_user IS NOT NULL
      THEN
      raise_application_error(-20006, 'Parameters P_SCHEMA and P_USER are mutually exclusive');
      WHEN p_schema IS NOT NULL
      THEN
      l_sel_grant := p_schema||'_sel';
      l_adm_grant := p_schema||'_adm';
      WHEN p_user IS NOT NULL
      THEN
      l_sel_grant := p_user;
      l_adm_grant := p_user;
      ELSE
      NULL;
      END CASE;

      BEGIN
	    EXECUTE IMMEDIATE 'GRANT SELECT ON FILES_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON FILES_DETAIL TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_DETAIL TO '||l_adm_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT ON files_detail_seq TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT ON files_detail_seq TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON FILES_OBJ_DETAIL TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON FILES_OBJ_DETAIL TO '||l_adm_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT ON files_obj_detail_seq TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT ON files_obj_detail_seq TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON TD_PART_GTT TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TD_PART_GTT TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON TD_BUILD_IDX_GTT TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TD_BUILD_IDX_GTT TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON TD_BUILD_CON_GTT TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TD_BUILD_CON_GTT TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON TD_CON_MAINT_GTT TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON TD_CON_MAINT_GTT TO '||l_adm_grant;
	 
	    EXECUTE IMMEDIATE 'GRANT SELECT ON OPT_STATS TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON OPT_STATS TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON DIMENSION_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON DIMENSION_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON MAPPING_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON MAPPING_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON COLUMN_CONF TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON COLUMN_CONF TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON COLUMN_TYPE_LIST TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON COLUMN_TYPE_LIST TO '||l_adm_grant;

	    EXECUTE IMMEDIATE 'GRANT SELECT ON REPLACE_METHOD_LIST TO '||l_sel_grant;
	    EXECUTE IMMEDIATE 'GRANT SELECT,UPDATE,DELETE,INSERT ON REPLACE_METHOD_LIST TO '||l_adm_grant;
	 
      EXCEPTION
	 WHEN e_no_grantee
	 THEN
	    raise_application_error(-20005,'The grantees '||l_sel_grant||' and '||l_adm_grant||' do not exist.');
      END;

   END grant_transcend_rep_privs;
   
   PROCEDURE grant_transcend_app_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      l_app_role VARCHAR2(30) := p_schema||'_app';
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
      e_role_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
      e_no_grantee   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_grantee, -1919 );
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
   BEGIN
      
      BEGIN
	 -- types
 	 EXECUTE IMMEDIATE 'grant execute on FILE_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on FEED_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on EXTRACT_OT to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on DIMENSION_OT to '||l_app_role;
	 --packages
	 EXECUTE IMMEDIATE 'grant execute on TD_DBUTILS to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TRANS_ADM to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TRANS_ETL to '||l_app_role;
	 EXECUTE IMMEDIATE 'grant execute on TRANS_FILES to '||l_app_role;
	 
      EXCEPTION
	 WHEN e_no_obj
	 THEN
	 raise_application_error(-20004,'Some application objects do not exist.');
      END;

   END grant_transcend_app_privs;
  
   PROCEDURE build_sys_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   BEGIN

      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      create_user( p_user 	=> p_schema, 
		   p_tablespace => p_tablespace );
      
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );
      
      -- this will drop all the tables before beginning
      IF p_drop
      THEN
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE users|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
       	       NULL;
	 END;
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE applications|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	       NULL;
	 END;
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE repositories|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	       NULL;
	 END;
      END IF;
      
      -- create all the tables
      BEGIN
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE repositories
	 ( 
	   repository_name     VARCHAR2(30) NOT NULL,
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE repositories ADD 
	 (
           CONSTRAINT repositories_pk
           PRIMARY KEY
	   (repository_name)
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE applications
	 ( 
	   application_name    VARCHAR2(30) NOT NULL,
	   repository_name     VARCHAR2(30) NOT NULL,
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE applications ADD 
	 (
	   CONSTRAINT applications_pk
	   PRIMARY KEY
	   (application_name)
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE applications ADD 
	 (
	   CONSTRAINT applications_fk1
	   FOREIGN KEY (repository_name)
	   REFERENCES repositories  
	   ( repository_name )
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE users
	 ( 
	   user_name           VARCHAR2(30) NOT NULL,
	   application_name    VARCHAR2(30) NOT NULL,
	   repository_name     VARCHAR2(30) NOT NULL,
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_pk
	   PRIMARY KEY
	   (user_name)
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_fk1
	   FOREIGN KEY (repository_name)
	   REFERENCES repositories  
	   ( repository_name )
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_fk2
	   FOREIGN KEY (application_name)
	   REFERENCES applications 
	   ( application_name )
	 )|';
      EXCEPTION
	 WHEN e_tab_exists
	 THEN
	 raise_application_error(-20003,'Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;
      
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set CURRENT_SCHEMA back to where it started
      reset_current_schema;
   EXCEPTION
   WHEN others
      THEN
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set CURRENT_SCHEMA back to where it started
      reset_current_schema;
      RAISE;      

   END build_sys_repo;

   PROCEDURE build_evolve_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
      e_no_seq   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_seq, -2289 );
   BEGIN
      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      create_user( p_user 	=> p_schema, 
		   p_tablespace => p_tablespace );
      
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );
      
      -- this will drop all the tables before beginning
      IF p_drop
      THEN
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE dir_list|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE runmode_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE parameter_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE count_table|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	      NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE error_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE log_table|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE logging_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE notification_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE notification_events|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE registration_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;
	 
      END IF;


      BEGIN      
	 -- DIR_LIST table
	 EXECUTE IMMEDIATE 
	 q'|CREATE global TEMPORARY TABLE dir_list
	 ( 
	   filename VARCHAR2(255),
	   file_dt date,
	   file_size NUMBER
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- COUNT_TABLE table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE count_table
	 (
	   entry_ts       TIMESTAMP DEFAULT systimestamp NOT null,
	   client_info    VARCHAR2(64),
	   module         VARCHAR2(48),
	   action         VARCHAR2(32),
	   runmode 	  VARCHAR2(10) NOT NULL,
	   session_id     NUMBER NOT null,
	   row_cnt        NUMBER NOT null
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE count_table ADD 
	 (
	   CONSTRAINT count_table_pk
	   PRIMARY KEY
	   (session_id,entry_ts)
	   USING INDEX
	 )|';
	 
	 -- ERROR_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE error_conf
	 ( 
	   code              NUMBER NOT NULL,
	   name 	     VARCHAR2(30) NOT NULL,
	   message 	     VARCHAR2(1000) NOT NULL,
	   comments 	     VARCHAR2(4000),
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE error_conf ADD 
	 (
	   CONSTRAINT error_conf_pk
	   PRIMARY KEY
	   (name)
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE error_conf ADD 
	 (
	   CONSTRAINT error_conf_uk1
	   unique
	   (code)
	   USING INDEX
	 )|';

	 -- LOG_TABLE table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE log_table
	 ( 
	   entry_ts TIMESTAMP (6) DEFAULT systimestamp NOT NULL,
	   msg VARCHAR2(2000) NOT NULL,
	   client_info VARCHAR2(64),
	   module VARCHAR2(48),
	   action VARCHAR2(32),
	   service_name VARCHAR2(64),
	   runmode VARCHAR2(10) NOT NULL,
	   session_id NUMBER NOT NULL,
	   current_scn NUMBER NOT NULL,
	   instance_name VARCHAR2(30) NOT NULL,
	   machine VARCHAR2(100) NOT NULL,
	   dbuser VARCHAR2(30) NOT NULL,
	   osuser VARCHAR2(30) NOT NULL,
	   code NUMBER NOT NULL,
	   call_stack VARCHAR2(1024),
	   back_trace VARCHAR2(1024),
	   batch_id number
	 )|';

	 -- LOGGING_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE logging_conf
	 ( 
	   logging_level    NUMBER not NULL,
	   debug_level 	  NUMBER NOT NULL,
	   module 	  VARCHAR2(48) NOT NULL,
	   created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt       DATE DEFAULT SYSDATE NOT NULL,
	   modified_user    VARCHAR2(30),
	   modified_dt	  DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE logging_conf ADD 
	 (
	   CONSTRAINT logging_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

	 -- NOTIFICATION_EVENTS table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE notification_events
	 ( 
	   module              VARCHAR2(48) NOT NULL,
	   action    	     VARCHAR2(32) NOT NULL,
	   subject             VARCHAR2(100) NOT NULL,
	   message             VARCHAR2(2000) NOT NULL,
	   created_user        VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	   created_dt   	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user       VARCHAR2(30),
	   modified_dt         DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_events ADD
	 (
	   CONSTRAINT notification_events_pk
	   PRIMARY KEY
	   ( action, module )
	   USING INDEX
	 )|';
	 
	 -- NOTIFICATION_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE notification_conf
	 ( 
	   label		   VARCHAR2(40) NOT NULL,
	   module        	   VARCHAR2(48) NOT NULL,
	   action        	   VARCHAR2(32) NOT NULL,
	   method      	   VARCHAR2(20) NOT NULL,
	   enabled     	   VARCHAR2(3) DEFAULT 'yes',
	   required	   VARCHAR2(3) DEFAULT 'no',
	   sender            VARCHAR2(1024),
	   recipients        VARCHAR2(2000) NOT NULL,
	   created_user      VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	   created_dt   	   DATE DEFAULT SYSDATE NOT NULL,
	   modified_user     VARCHAR2(30),
	   modified_dt       DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD
	 (
	   CONSTRAINT notification_conf_pk
	   PRIMARY KEY
	   ( label,module,action )
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD 
	 (
	   CONSTRAINT notification_conf_fk1
	   FOREIGN KEY ( module, action )
	   REFERENCES notification_events
	   ( module, action )
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck1 CHECK (module=lower(module))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck2 CHECK (action=lower(action))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck3 CHECK (method=lower(method))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck4 CHECK (enabled=lower(enabled))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck5 CHECK (required=lower(required))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck6 CHECK (sender=lower(sender))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck7 CHECK (recipients=lower(recipients))|';
	 
	 -- REGISTRATION_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE registration_conf
	 ( 
	   registration  	     VARCHAR2(10) NOT NULL,
	   module 	     VARCHAR2(48),
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE registration_conf ADD 
	 (
	   CONSTRAINT registration_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck1 CHECK (module=lower(module))|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck2 CHECK (registration=lower(registration))|';

	 -- RUNMODE_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE runmode_conf
	 ( 
	   default_runmode  VARCHAR2(10) not NULL,
	   module 	  VARCHAR2(48),
	   created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt       DATE DEFAULT SYSDATE NOT NULL,
	   modified_user    VARCHAR2(30),
	   modified_dt	  DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE runmode_conf ADD 
	 (
	   CONSTRAINT runmode_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE runmode_conf ADD CONSTRAINT runmode_conf_ck1 CHECK (module=lower(module))|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE runmode_conf ADD CONSTRAINT runmode_conf_ck2 CHECK (default_runmode=lower(default_runmode))|';

	 -- PARAMETER_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE parameter_conf
	 ( 
	   name		VARCHAR2(40) NOT NULL,
	   value 		VARCHAR2(40),
	   module 	VARCHAR2(48) NOT NULL,
	   created_user   VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  VARCHAR2(30),
	   modified_dt    DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE parameter_conf ADD 
	 (
	   CONSTRAINT parameter_conf_pk
	   PRIMARY KEY
	   (name,module)
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck1 CHECK (lower(value) <> 'default')|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck2 CHECK (value=lower(value))|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck3 CHECK (module=lower(module))|';
	 
	 -- grant the privileges to the repository tables to the roles
	 grant_evolve_rep_privs( p_schema => p_schema, 
	 			 p_drop	  => p_drop );
	 
	 -- write application tracking record
	 EXECUTE IMMEDIATE 	    
	 q'|UPDATE tdsys.repositories
	 SET modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	 modified_dt = SYSDATE
	 WHERE repository_name=upper(:v_schema)|'
	 USING p_schema;
	 
	 IF SQL%ROWCOUNT = 0
	 THEN
	    EXECUTE IMMEDIATE
	    q'|INSERT INTO tdsys.repositories
	    ( repository_name)
	    VALUES
	    ( upper(:v_schema))|'
	    USING p_schema;
	 END IF;


      EXCEPTION
	 WHEN e_tab_exists
	 THEN
	 raise_application_error(-20003,'Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;
      
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
   WHEN others
      THEN
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to where it started
      reset_current_schema;
      RAISE;      

   END build_evolve_repo;

   PROCEDURE build_transcend_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_stat_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_stat_tab_exists, -20002 );
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
      e_no_seq   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_seq, -2289 );
   BEGIN
      
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- this will drop all the tables before beginning
      IF p_drop
      THEN
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE column_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE dimension_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE mapping_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE column_type_list|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	      NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE replace_method_list|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE files_obj_detail|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE files_detail|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE files_conf|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE td_part_gtt|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE td_build_idx_gtt|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE td_build_con_gtt|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE td_con_maint_gtt|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP TABLE opt_stats|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP sequence files_detail_seq|';
	 EXCEPTION
	    WHEN e_no_seq
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP sequence files_obj_detail_seq|';
	 EXCEPTION
	    WHEN e_no_seq
	    THEN
	    NULL;
	 END;
	 
      END IF;


      BEGIN
	 -- create the statitics table
	 DBMS_STATS.CREATE_STAT_TABLE( p_schema, 'OPT_STATS' );

	 -- FILES_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE files_conf
	 ( 
	   file_label	       VARCHAR2(100) 	NOT NULL,
	   file_group	       VARCHAR2(64) 	NOT NULL,
	   file_type	       VARCHAR2(7) 	NOT NULL,
	   file_description    VARCHAR2(100),
	   object_owner	       VARCHAR2(30)	NOT NULL,
	   object_name	       VARCHAR2(30)    	NOT NULL,
	   directory	       VARCHAR2(30)	NOT NULL,
	   filename	       VARCHAR2(50)    	NOT NULL,		
	   arch_directory      VARCHAR2(30) 	NOT NULL,
	   min_bytes	       NUMBER 		DEFAULT 0 NOT NULL,
	   max_bytes           NUMBER 		DEFAULT 0 NOT NULL,
	   file_datestamp      VARCHAR2(30),
	   baseurl             VARCHAR2(500),
	   passphrase          VARCHAR2(100),
	   source_directory    VARCHAR2(50),
	   source_regexp       VARCHAR2(100),
	   regexp_options      VARCHAR2(10),
	   source_policy       VARCHAR2(10),
	   required            VARCHAR2(3),
	   delete_source       VARCHAR2(3),
	   reject_limit        NUMBER,
	   dateformat	       VARCHAR2(30),
	   timestampformat     VARCHAR2(30),
	   delimiter	       VARCHAR2(3),
	   quotechar	       VARCHAR2(2),
	   headers	       VARCHAR2(3),
	   created_user        VARCHAR2(30),
	   created_dt          DATE,
	   modified_user       VARCHAR2(30),
	   modified_dt         DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_conf ADD 
	 (
	   CONSTRAINT files_conf_pk
	   PRIMARY KEY
	   (file_label, file_group)
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_conf ADD 
	 (
	   CONSTRAINT files_conf_ck2
	   CHECK (source_policy IN ('oldest','newest','all','fail',NULL))
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_conf ADD 
	   CONSTRAINT files_conf_ck2
	   CHECK (file_type = case when source_directory is null or source_regexp is null then 'extract' ELSE file_type END )|';
	 
	 -- FILES_DETAIL table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE files_detail
	 ( 
	   file_detail_id	NUMBER		NOT NULL,
	   file_label 	VARCHAR2(50),
	   file_group 	VARCHAR2(64),
	   file_type 	VARCHAR2(7)	NOT null,
	   source_filepath VARCHAR2(200),
	   target_filepath VARCHAR2(200),
	   arch_filepath 	VARCHAR2(100)	NOT NULL,
	   num_bytes 	NUMBER 		NOT NULL,
	   num_lines 	NUMBER,
	   file_dt 	DATE NOT	NULL,
	   PROCESSED_TS 	TIMESTAMP 	DEFAULT systimestamp NOT NULL,
	   session_id 	NUMBER 		DEFAULT sys_context('USERENV','SESSIONID') NOT NULL
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_detail ADD 
	 (
	   CONSTRAINT file_detail_pk
	   PRIMARY KEY
	   (file_detail_id)
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_detail ADD 
	 (
	   CONSTRAINT file_detail_fk1
	   FOREIGN KEY ( file_label, file_group )
	   REFERENCES files_conf
	   ( file_label, file_group )
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|CREATE SEQUENCE files_detail_seq|';
	 
	 -- FILES_OBJ_DETAIL table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE files_obj_detail
	 ( 
	   file_obj_detail_id    NUMBER NOT NULL,
	   file_label 	         VARCHAR2(30) NOT NULL,
	   file_group 	      	 VARCHAR2(50) NOT NULL,
	   file_type 	      	 VARCHAR2(7) NOT NULL,
	   object_owner  	 VARCHAR2(30) NOT NULL,
	   object_name  	 VARCHAR2(30) NOT NULL,
	   processed_ts 	 TIMESTAMP DEFAULT systimestamp NOT NULL,
	   num_rows 	      	 NUMBER,
	   num_lines 	      	 NUMBER,
	   percent_diff 	 NUMBER,
	   session_id 	      	 NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT NULL
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_obj_detail ADD 
	 (
	   CONSTRAINT files_obj_detail_pk
	   PRIMARY KEY
	   (file_obj_detail_id)
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|CREATE SEQUENCE files_obj_detail_seq|';

	 -- TD_PART_GTT table
	 EXECUTE IMMEDIATE 
	 q'|CREATE global TEMPORARY TABLE td_part_gtt
	 ( 
	   table_owner VARCHAR2(30),
	   table_name VARCHAR2(30),
	   partition_name VARCHAR2(30),
	   partition_position NUMBER
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- TD_BUILD_IDX_GTT
	 EXECUTE IMMEDIATE 
	 q'|CREATE global TEMPORARY TABLE td_build_idx_gtt
	 ( 
	   rename_ddl 	      VARCHAR2(4000),
	   rename_msg 	      VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- TD_BUILD_CON_GTT
	 EXECUTE IMMEDIATE
	 q'|CREATE global TEMPORARY TABLE td_build_con_gtt
	 ( 
	   rename_ddl 	      VARCHAR2(4000),
	   rename_msg 	      VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- TD_CON_MAINT_GTT
	 EXECUTE IMMEDIATE
	 q'|CREATE global TEMPORARY TABLE td_con_maint_gtt
	 ( 
	   disable_ddl 	    VARCHAR2(4000),
	   disable_msg 	    VARCHAR2(4000),
	   enable_ddl 	    VARCHAR2(4000),
	   enable_msg 	    VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';

	 -- REPLACE_METHOD_LIST table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE replace_method_list
	 ( 
	   replace_method	VARCHAR2(10) NOT NULL,
	   created_user	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE replace_method_list ADD 
	 (
	   CONSTRAINT replace_method_list_pk
	   PRIMARY KEY
	   ( replace_method )
	   USING INDEX
	 )|';

	 EXECUTE IMMEDIATE q'|INSERT INTO replace_method_list (replace_method) VALUES ('exchange')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO replace_method_list (replace_method) VALUES ('rename')|';
	 
	 -- COLUMN_TYPE_LIST table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE column_type_list
	 ( 
	   column_type	VARCHAR2(30) NOT NULL,
	   created_user	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE column_type_list ADD 
	 (
	   CONSTRAINT column_type_list_pk
	   PRIMARY KEY
	   ( column_type )
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('surrogate key')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('natural key')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('scd type 1')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('scd type 2')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('effective date')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('expiration date')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('current indicator')|';
	 
	 -- MAPPING_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE mapping_conf
	 ( 
	   mapping_name		VARCHAR2(30),
	   table_owner 		VARCHAR2(61),
	   table_name 		VARCHAR2(30),
	   partition_name	VARCHAR2(30),
	   manage_indexes 	VARCHAR2(3) NOT NULL,
	   manage_constraints 	VARCHAR2(3) NOT NULL,
	   source_owner 	VARCHAR2(30),
	   source_object 	VARCHAR2(30),
	   source_column 	VARCHAR2(30),
	   replace_method 	VARCHAR2(10),
	   statistics 		VARCHAR2(10),
	   concurrent 		VARCHAR2(3) NOT NULL,
	   index_regexp 	VARCHAR2(30),
	   index_type 		VARCHAR2(30),
	   partition_type	VARCHAR2(30),
	   constraint_regexp 	VARCHAR2(100),
	   constraint_type 	VARCHAR2(100),
	   description		VARCHAR2(2000),
	   created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD 
	 (
	   CONSTRAINT mapping_conf_pk
	   PRIMARY KEY
	   ( mapping_name )
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck1 CHECK (mapping_name=lower(mapping_name))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck2 CHECK (manage_indexes in ('yes','no'))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck3 CHECK (manage_constraints in ('yes','no'))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck4 CHECK (concurrent in ('yes','no'))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck5 CHECK (replace_method in ('exchange','rename'))|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck6 CHECK (replace_method = case when table_owner <> source_owner then 'exchange' else replace_method end )|';
	 
	 -- DIMENSION_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE dimension_conf
	 ( 
	   table_owner		VARCHAR2(30) NOT NULL,
	   table_name		VARCHAR2(30) NOT NULL,
	   source_owner		VARCHAR2(30) NOT NULL,
	   source_object	VARCHAR2(30) NOT NULL,
	   sequence_owner  	VARCHAR2(30) NOT NULL,
	   sequence_name  	VARCHAR2(30) NOT NULL,
	   staging_owner	VARCHAR2(30) DEFAULT NULL,
	   staging_table	VARCHAR2(30) DEFAULT NULL,
	   default_scd_type	NUMBER(1,0) DEFAULT 2 NOT NULL,
	   direct_load		VARCHAR2(3) DEFAULT 'yes' NOT NULL,
	   replace_method	VARCHAR2(10) DEFAULT 'rename' NOT NULL,
	   statistics		VARCHAR2(10) DEFAULT 'transfer',
	   concurrent		VARCHAR2(3) DEFAULT 'yes' NOT NULL,
	   stage_key_default	NUMBER DEFAULT -.01 NOT NULL,
	   char_nvl_default	VARCHAR2(1000) DEFAULT '~' NOT NULL,
	   date_nvl_default	DATE DEFAULT to_date('01/01/9999','mm/dd/yyyy') NOT NULL,
	   number_nvl_default	NUMBER DEFAULT -.01 NOT NULL,
	   description		VARCHAR2(2000) DEFAULT NULL,
	   created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';	 
	 	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE dimension_conf ADD 
	 (
	   CONSTRAINT dimension_conf_pk
	   PRIMARY KEY
	   ( table_owner, table_name )
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE dimension_conf ADD 
	 (
	   CONSTRAINT dimension_conf_fk1
	   FOREIGN KEY ( replace_method )
	   REFERENCES replace_method_list
	   ( replace_method )
	 )|';

	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE dimension_conf ADD 
	 ( CONSTRAINT replace_method_ck
	   CHECK ( upper(staging_owner) = CASE WHEN replace_method = 'rename' THEN upper(table_owner) ELSE staging_owner end )
	 )|';

	 -- COLUMN_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE column_conf
	 ( 
	   table_owner		VARCHAR2(30) NOT NULL,
	   table_name	VARCHAR2(30) NOT NULL,
	   column_name	VARCHAR2(30) NOT NULL,
	   column_type	VARCHAR2(30) NOT NULL,
	   created_user	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_pk
	   PRIMARY KEY
	   ( table_owner, table_name, column_name )
	   USING INDEX
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_fk1
	   FOREIGN KEY ( column_type )
	   REFERENCES column_type_list
	   ( column_type )
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_fk2
	   FOREIGN KEY ( table_owner, table_name )
	   REFERENCES dimension_conf  
	   ( table_owner, table_name )
	 )|';
	 
	 -- grant the privileges to the repository tables to the roles
	 grant_transcend_rep_privs( p_schema => p_schema ); 
     
      EXCEPTION
      WHEN e_tab_exists OR e_stat_tab_exists
      THEN
	 raise_application_error(-20003,'Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;
 
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
   WHEN others
      THEN
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to where it started
      reset_current_schema;
      RAISE;      

   END build_transcend_repo;
   
   PROCEDURE build_evolve_rep_syns(
      p_user        VARCHAR2,
      p_schema  VARCHAR2
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_same_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_same_name, -1471 );
   BEGIN      
      -- create TDSYS synonyms
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.REPOSITORIES for TDSYS.REPOSITORIES';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.APPLICATIONS for TDSYS.APPLICATIONS';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.USERS for TDSYS.USERS';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 -- create the repository synonyms
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.COUNT_TABLE for '||p_schema||'.COUNT_TABLE';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.DIR_LIST for '||p_schema||'.DIR_LIST';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.ERROR_CONF for '||p_schema||'.ERROR_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.LOGGING_CONF for '||p_schema||'.LOGGING_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.LOG_TABLE for '||p_schema||'.LOG_TABLE';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.NOTIFICATION_CONF for '||p_schema||'.NOTIFICATION_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.NOTIFICATION_EVENTS for '||p_schema||'.NOTIFICATION_EVENTS';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.REGISTRATION_CONF for '||p_schema||'.REGISTRATION_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.RUNMODE_CONF for '||p_schema||'.RUNMODE_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.PARAMETER_CONF for '||p_schema||'.PARAMETER_CONF';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;   
   END build_evolve_rep_syns;

   PROCEDURE build_evolve_app_syns(
      p_user        VARCHAR2,
      p_schema  VARCHAR2
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_same_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_same_name, -1471 );
   BEGIN
      -- create the synonyms
      -- types
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.APP_OT for '||p_schema||'.APP_OT';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.EVOLVE_OT for '||p_schema||'.EVOLVE_OT';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.NOTIFICATION_OT for '||p_schema||'.NOTIFICATION_OT';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.SPLIT_OT for '||p_schema||'.SPLIT_OT';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
 
      -- packages and functions
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.STRAGG for '||p_schema||'.STRAGG';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_CORE for '||p_schema||'.TD_CORE';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_INST for '||p_schema||'.TD_INST';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;	 

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.EVOLVE_LOG for '||p_schema||'.EVOLVE_LOG';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_UTILS for '||p_schema||'.TD_UTILS';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.EVOLVE_APP for '||p_schema||'.EVOLVE_APP';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.EVOLVE_ADM for '||p_schema||'.EVOLVE_ADM';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;
	 
	 -- sequences
	 BEGIN
	    EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.CONCURRENT_ID_SEQ for '||p_schema||'.CONCURRENT_ID_SEQ';
	 EXCEPTION
	    WHEN e_same_name
	    THEN
	    NULL;
	 END;

   END build_evolve_app_syns;
   
   PROCEDURE build_transcend_rep_syns(
      p_user     VARCHAR2,
      p_schema   VARCHAR2
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_same_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_same_name, -1471 );
   BEGIN
      -- create the synonyms
      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_CONF for '||p_schema||'.FILES_CONF';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_DETAIL for '||p_schema||'.FILES_DETAIL';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_DETAIL_SEQ for '||p_schema||'.FILES_DETAIL_SEQ';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_OBJ_DETAIL for '||p_schema||'.FILES_OBJ_DETAIL';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_OBJ_DETAIL_SEQ for '||p_schema||'.FILES_OBJ_DETAIL_SEQ';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_PART_GTT for '||p_schema||'.TD_PART_GTT';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_BUILD_IDX_GTT for '||p_schema||'.TD_BUILD_IDX_GTT';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_BUILD_CON_GTT for '||p_schema||'.TD_BUILD_CON_GTT';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_CON_MAINT_GTT for '||p_schema||'.TD_CON_MAINT_GTT';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.COLUMN_CONF for '||p_schema||'.COLUMN_CONF';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.DIMENSION_CONF for '||p_schema||'.DIMENSION_CONF';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.MAPPING_CONF for '||p_schema||'.MAPPING_CONF';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.COLUMN_TYPE_LIST for '||p_schema||'.COLUMN_TYPE_LIST';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.REPLACE_METHOD_LIST for '||p_schema||'.REPLACE_METHOD_LIST';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.OPT_STATS for '||p_schema||'.OPT_STATS';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      -- create the synonyms for the sequences in the repository
      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_DETAIL_SEQ for '||p_schema||'.FILES_DETAIL_SEQ';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.FILES_OBJ_DETAIL_SEQ for '||p_schema||'.FILES_OBJ_DETAIL_SEQ';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;
   END build_transcend_rep_syns;

   PROCEDURE build_transcend_app_syns(
      p_user    VARCHAR2,
      p_schema  VARCHAR2
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_same_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_same_name, -1471 );
   BEGIN
      -- create the synonyms
      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TD_DBUTILS for '||p_schema||'.TD_DBUTILS';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TRANS_ETL for '||p_schema||'.TRANS_ETL';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TRANS_FILES for '||p_schema||'.TRANS_FILES';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;
      
      BEGIN
	 EXECUTE IMMEDIATE 'create or replace synonym '||p_user||'.TRANS_ADM for '||p_schema||'.TRANS_ADM';
      EXCEPTION
	 WHEN e_same_name
	 THEN
	 NULL;
      END;

   END build_transcend_app_syns;

   PROCEDURE grant_evolve_sys_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS',
      p_drop     BOOLEAN  DEFAULT FALSE    
   ) 
   IS
      l_sys_role VARCHAR2(30)  := p_schema||'_sys';
      l_java_role VARCHAR2(30) := p_schema||'_java';
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
      e_role_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
      e_no_role   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
   BEGIN      
      -- this will drop the roles before beginning
      IF p_drop
      THEN
	 BEGIN
	    EXECUTE IMMEDIATE 'DROP role '||l_sys_role;
	 EXCEPTION
	    WHEN e_no_role
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE 'DROP role '||l_java_role;
	 EXCEPTION
	    WHEN e_no_role
	    THEN
	    NULL;
	 END;
	 
      END IF;

      BEGIN
	 EXECUTE IMMEDIATE 'CREATE ROLE '||l_sys_role;
      EXCEPTION
	 WHEN e_role_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'CREATE ROLE '||l_java_role;
      EXCEPTION
	 WHEN e_role_exists
	 THEN
	 NULL;
      END;
	 
      -- for each system privilege, grant it to the application owner and the _SYS role	    
      EXECUTE IMMEDIATE 'GRANT CONNECT TO '||l_sys_role;
      EXECUTE IMMEDIATE 'GRANT CONNECT TO '||p_schema;
      EXECUTE IMMEDIATE 'GRANT RESOURCE TO '||l_sys_role;
      EXECUTE IMMEDIATE 'GRANT RESOURCE TO '||p_schema;
      EXECUTE IMMEDIATE 'GRANT ALTER SESSION TO '||l_sys_role;
      EXECUTE IMMEDIATE 'GRANT ALTER SESSION TO '||p_schema;
      EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO '||l_sys_role;
      EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO '||p_schema;
      
      -- grant permissions on UTL_MAIL
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN

	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.utl_mail TO '||l_sys_role;
	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.utl_mail TO '||p_schema;
	 
      EXCEPTION
	 WHEN e_no_obj
	 THEN
	 raise_application_error(-20009, 'Either package UTL_MAIL does not exist, or the installing user does not have privileges to grant access on it.');
      END;
      
      -- grant permissions on DBMS_LOCK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN

	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_lock TO '||l_sys_role;
	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_lock TO '||p_schema;
	 
      EXCEPTION
	 WHEN e_no_obj
	 THEN
	 raise_application_error(-20009, 'Either package DBMS_LOCK does not exist, or the installing user does not have privileges to grant access on it.');
      END;
      
      -- grant permissions on DBMS_FLASHBACK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN

	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_flashback TO '||l_sys_role;
	 EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_flashback TO '||p_schema;
	 
      EXCEPTION
	 WHEN e_no_obj
	 THEN
	 raise_application_error(-20009, 'Either package DBMS_FLASHBACK does not exist, or the installing user does not have privileges to grant access on it.');
      END;

      -- grant java specific privilege to the _JAVA role
      dbms_java.set_output(1000000);
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
      dbms_java.grant_permission( upper(l_java_role), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor','' );
      
      -- grant the _JAVA role to the app owner and the _APP role
      EXECUTE IMMEDIATE 'GRANT '||l_java_role||' TO '||l_sys_role;
      EXECUTE IMMEDIATE 'GRANT '||l_java_role||' TO '||p_schema;
   EXCEPTION
   WHEN others
      THEN
	 -- set current_schema back to where it started
      reset_current_schema;
      RAISE;      

   END grant_evolve_sys_privs;
   
   PROCEDURE grant_transcend_sys_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      l_sys_role VARCHAR2(30)  := p_schema||'_sys';
      l_app_role VARCHAR2(30)  := p_schema||'_app';
      l_java_role VARCHAR2(30) := p_schema||'_java';
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
      e_role_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
      e_no_role   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
      e_no_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_obj, -942 );
   BEGIN
	 BEGIN
	    -- for each system privilege, grant it to the application owner and the _SYS role
	    EXECUTE IMMEDIATE 'GRANT ALTER ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT INSERT ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT SELECT ANY dictionary TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT SELECT ANY SEQUENCE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT UPDATE ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT DELETE ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT ALTER ANY INDEX TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT CREATE ANY INDEX TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT DROP ANY INDEX TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT DROP ANY TABLE TO '||p_schema||'_sys';
	    EXECUTE IMMEDIATE 'GRANT ANALYZE ANY TO '||p_schema||'_sys';
	    
      EXCEPTION
	 WHEN e_no_obj
	    THEN
	    raise_application_error(-20004,'Some repository objects do not exist.');
      END;

   END grant_transcend_sys_privs;

   
   PROCEDURE build_evolve_app(
      p_schema       VARCHAR2 DEFAULT 'TDSYS',
      p_repository   VARCHAR2 DEFAULT 'TDSYS',
      p_drop	     BOOLEAN  DEFAULT FALSE
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user 	=> p_schema );
      
      -- two packages that are needed
      
      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );
      
      -- create grants to the application owner to all the tables in the repository
      grant_evolve_rep_privs( p_user => p_schema );
      
      -- set the CURRENT_SCHEMA back
      reset_current_schema;
      
      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );

      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user   => p_schema,
			     p_schema => p_repository );
      
      -- grant application privileges to the roles
      grant_evolve_sys_privs( p_schema => p_schema );

      -- create the dbms_scheduler program
      create_scheduler_metadata;
      
      -- create a sequence for concurrent ids
      -- this is created in the application schema because it is not associated with any tables
      -- if there were multiple repositories being used, the value generated by the sequence would need to be unique across them
      EXECUTE IMMEDIATE 
      q'|CREATE SEQUENCE concurrent_id_seq|';
      	 	 
      -- write application tracking record
      EXECUTE IMMEDIATE 	    
      q'|UPDATE tdsys.applications
      SET repository_name = upper(:v_rep_schema),
      modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
      modified_dt = SYSDATE
      WHERE application_name=upper(:v_app_schema)|'
      USING p_repository, p_schema;
      
      IF SQL%ROWCOUNT = 0
      THEN
	 EXECUTE IMMEDIATE
	 q'|INSERT INTO tdsys.applications
	 ( application_name,
	   repository_name)
	 VALUES
	 ( upper(:v_app_schema),
	   upper(:v_rep_schema))|'
	 USING p_schema, p_repository;
      END IF;
      
      dbms_output.put_line(' The CURRENT_SCHEMA is set to '||sys_context('USERENV','CURRENT_SCHEMA')||' in preparation for installing application');     

   END build_evolve_app;
   
   PROCEDURE build_transcend_app(
      p_schema       VARCHAR2 DEFAULT 'TDSYS',
      p_repository   VARCHAR2 DEFAULT 'TDSYS',
      p_drop	     BOOLEAN  DEFAULT FALSE
   ) 
   IS
      e_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
      e_no_tab   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   BEGIN
      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );
      
      -- create grants to the application owner to all the tables in the repository
      grant_transcend_rep_privs( p_user => p_schema );
      
      -- set the CURRENT_SCHEMA back
      reset_current_schema;
      
      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );

      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user   => p_schema,
				p_schema => p_repository );
      
      -- grant application privileges to the roles
      grant_transcend_sys_privs( p_schema => p_schema );
      
   END build_transcend_app;
   
   PROCEDURE drop_evolve_types
   IS
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -4043 );
   BEGIN
      
      -- there are Transcend types that inherit from Evolve types
      -- so we need to drop any Transcend types first

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE notification_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE evolve_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE app_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;
	 
   END drop_evolve_types;

   PROCEDURE drop_transcend_types
   IS
      e_obj_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_obj_exists, -4043 );
   BEGIN

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE dimension_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE feed_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE extract_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;

      BEGIN
	 EXECUTE IMMEDIATE 'DROP TYPE file_ot';
      EXCEPTION
	 WHEN e_obj_exists
	 THEN
	 NULL;
      END;
	 
   END drop_transcend_types;

   PROCEDURE create_evolve_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT 'TDSYS', 
      p_repository   VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user  => p_user );
      
      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user   => p_user,
			     p_schema => p_repository );

      -- create the synonyms to the application
      build_evolve_app_syns( p_user   => p_user,
			     p_schema => p_application );
      
      -- grant the appropriate roles to the application user
      EXECUTE IMMEDIATE 'grant '||p_repository||'_adm to '||p_user;
      EXECUTE IMMEDIATE 'grant '||p_application||'_app to '||p_user;

      -- write application tracking record
      EXECUTE IMMEDIATE 	    
      q'|UPDATE tdsys.users
      SET application_name = upper(:v_app_schema),
      repository_name = upper(:v_rep_schema),
      modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
      modified_dt = SYSDATE
      WHERE user_name=upper(:v_user)|'
      USING p_application, p_repository,p_user;
      
      IF SQL%ROWCOUNT = 0
      THEN
	 EXECUTE IMMEDIATE
	 q'|INSERT INTO tdsys.users
	 ( user_name,
	   application_name,
	   repository_name)
	 VALUES
	 ( upper(:v_user),
	   upper(:v_app_schema),
	   upper(:v_rep_schema))|'
	 USING p_user, p_application, p_repository;
      END IF;
      
   END create_evolve_user;

   PROCEDURE create_transcend_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT 'TDSYS', 
      p_repository   VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user  => p_user );
      
      EXECUTE IMMEDIATE 'grant select_catalog_role to '||p_user;
      
      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user   => p_user,
				p_schema => p_repository );

      -- create the synonyms to the application
      build_transcend_app_syns( p_user   => p_user,
				p_schema => p_application );
      
   END create_transcend_user;   

END td_install;
/

SHOW errors
