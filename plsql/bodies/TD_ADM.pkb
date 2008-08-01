CREATE OR REPLACE PACKAGE BODY td_adm
IS
   g_user             dba_users.username%TYPE;
   g_tablespace       dba_users.default_tablespace%TYPE;
   g_current_schema   dba_users.username%TYPE := SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' );

   -- exceptions used over and over agaoin
   -- define them only once
   e_no_user          EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_user, -1435 );
   e_obj_exists    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
   e_role_exists   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
   e_no_grantee    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_grantee, -1919 );
   e_no_obj        EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_obj, -4043 );
   e_tab_exists   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
   e_no_tab       EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   e_no_seq       EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_seq, -2289 );
   e_same_name    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_same_name, -1471 );


   PROCEDURE create_user( p_user VARCHAR2 DEFAULT DEFAULT_REPOSITORY, p_tablespace VARCHAR2 DEFAULT NULL )
   IS
      l_user           all_users.username%TYPE;
      l_def_tbs        database_properties.property_value%TYPE;
      l_ddl	       LONG;
   BEGIN
      -- get the default tablespace
      SELECT property_value
        INTO l_def_tbs
        FROM database_properties
       WHERE property_name = 'DEFAULT_PERMANENT_TABLESPACE';

      -- find out if the user exists
      -- also get the current default tablespace of the user
      BEGIN
         SELECT default_tablespace
           INTO g_tablespace
           FROM dba_users
          WHERE username = UPPER( p_user );

	 IF p_tablespace IS NOT NULL
	 THEN
            g_user := p_user;

            EXECUTE IMMEDIATE 'alter user ' || p_user || ' default tablespace ' || p_tablespace;
	 END IF;	 

      EXCEPTION
         -- the user does not exist
         WHEN NO_DATA_FOUND
         THEN
	    l_ddl :=    'CREATE USER '
                     || p_user
                     || ' identified by no2'
                     || p_user
                     || CASE
                           WHEN p_tablespace IS NULL
                              THEN NULL
                           ELSE ' default tablespace ' || p_tablespace
                        END;
			
            -- therefore, we need to create it
	    EXECUTE IMMEDIATE l_ddl;
				 -- if we had to create the user, then it won't have CONNECT or a quote
            EXECUTE IMMEDIATE 'grant connect to ' || p_user;

            EXECUTE IMMEDIATE 'ALTER USER ' || p_user || ' QUOTA 50M ON ' || NVL( p_tablespace, l_def_tbs );

      END;

   END create_user;

   PROCEDURE set_current_schema( p_schema VARCHAR2 DEFAULT DEFAULT_REPOSITORY )
   IS
      l_current_schema   dba_users.username%TYPE;
   BEGIN
      BEGIN
         -- get the current schema before this
	 l_current_schema := SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' );

         -- set the session to that user
         EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema=' || p_schema;

         g_current_schema := l_current_schema;
      EXCEPTION
         WHEN e_no_user
         THEN
            raise_application_error( -20008, 'User "' || UPPER( p_schema ) || '" does not exist.' );
      END;
   END set_current_schema;

   PROCEDURE reset_default_tablespace
   IS
   BEGIN
      IF g_tablespace IS NOT NULL AND g_user IS NOT NULL
      THEN
         EXECUTE IMMEDIATE 'alter user ' || g_user || ' default tablespace ' || g_tablespace;

         g_tablespace := NULL;
         g_user := NULL;
      END IF;
   END reset_default_tablespace;

   PROCEDURE reset_current_schema
   IS
   BEGIN
      EXECUTE IMMEDIATE 'alter session set current_schema=' || g_current_schema;
   END reset_current_schema;

   -- this creates the job metadata (called a program) for submitting concurrent processes
   PROCEDURE create_scheduler_metadata
   IS
      e_no_sched_obj   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_sched_obj, -27476 );
   BEGIN
      -- first, drop the job class and the program
      BEGIN
         DBMS_SCHEDULER.drop_job_class( job_class_name => 'EVOLVE_DEFAULT_CLASS' );
      EXCEPTION
         WHEN e_no_sched_obj
         THEN
            NULL;
      END;

      DBMS_SCHEDULER.create_job_class
         ( job_class_name      => 'EVOLVE_DEFAULT_CLASS',
           logging_level       => DBMS_SCHEDULER.logging_full,
           comments            =>    'Job class for the Evolve product by Transcendent Data, Inc.'
                                  || ' This is the job class used by default when the Oracle scheduler is used for concurrent processing'
         );
   END create_scheduler_metadata;

   PROCEDURE grant_evolve_rep_privs(
      p_grantee   VARCHAR2,
      -- 'select' OR 'admin'
      p_mode	  VARCHAR2 DEFAULT 'admin'
   )
   IS
      l_grant	VARCHAR2(100);
   BEGIN      
      -- if p_mode is 'select', then only grant select privilege
      -- if it's 'admin', then grant all privileges
      l_grant := CASE p_mode WHEN 'select' THEN 'SELECT' ELSE 'SELECT,UPDATE,INSERT,DELETE' END;

      BEGIN
         -- first, the TDSYS tables
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TDSYS.REPOSITORIES TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TDSYS.APPLICATIONS TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TDSYS.USERS TO ' || p_grantee;

         -- now the evolve repository tables
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COUNT_TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON DIR_LIST TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON LOGGING_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON LOG_TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON NOTIFICATION_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON NOTIFICATION_EVENTS TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON PARAMETER_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON REGISTRATION_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON RUNMODE_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON ERROR_CONF TO ' || p_grantee;
	 
         -- sequences
         EXECUTE IMMEDIATE 'grant select on CONCURRENT_ID_SEQ to ' || p_grantee;

      EXCEPTION
         WHEN e_no_grantee
         THEN
            raise_application_error( -20005,
                                     'Grantees ' || p_grantee || ' does not exist.'
                                   );
         WHEN e_no_tab
         THEN
            raise_application_error( -20004, 'Some repository objects do not exist.' );
      END;
   END grant_evolve_rep_privs;

   PROCEDURE grant_evolve_app_privs( p_user VARCHAR2, p_schema VARCHAR2 DEFAULT DEFAULT_REPOSITORY )
   IS
   BEGIN
      EXECUTE IMMEDIATE 'grant execute on TDSYS.TD_ADM to ' || p_user;

      -- types
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.APP_OT to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.EVOLVE_OT to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.SPLIT_OT to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.STRAGG to ' || p_user;

      -- packages
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TD_INST to ' || p_user;
      
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.EVOLVE to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.EVOLVE_ADM to ' || p_user;

   EXCEPTION
      WHEN e_no_obj
      THEN
      raise_application_error( -20004, 'Some application objects do not exist.' );
   END grant_evolve_app_privs;

   PROCEDURE grant_transcend_rep_privs(
      p_grantee   VARCHAR2,
      -- 'select' OR 'admin'
      p_mode	  VARCHAR2 DEFAULT 'admin'
   BEGIN

      -- if p_mode is 'select', then only grant select privilege
      -- if it's 'admin', then grant all privileges
      l_grant := CASE p_mode WHEN 'select' THEN 'SELECT' ELSE 'SELECT,UPDATE,INSERT,DELETE' END;
	 
      BEGIN
	 
	 -- tables
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILES_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILES_DETAIL TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILES_OBJ_DETAIL TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TD_PART_GTT TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TD_BUILD_IDX_GTT TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TD_BUILD_CON_GTT TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TD_CON_MAINT_GTT TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON OPT_STATS TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON DIMENSION_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON MAPPING_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COLUMN_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COLUMN_TYPE_LIST TO ' || p_grantee;
	 
	 -- sequence
         EXECUTE IMMEDIATE 'GRANT SELECT ON files_detail_seq TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT SELECT ON files_obj_detail_seq TO ' || p_grantee;

      EXCEPTION
         WHEN e_no_grantee
         THEN
            raise_application_error( -20005,
                                     'The grantees ' || l_sel_grant || ' and ' || l_adm_grant || ' do not exist.'
                                   );
      END;
   END grant_transcend_rep_privs;

   PROCEDURE grant_transcend_app_privs( p_user VARCHAR2, p_schema VARCHAR2 DEFAULT DEFAULT_REPOSITORY )
   IS
   BEGIN
      --packages
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TRANS_ADM to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TRANS_ETL to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TRANS_FILES to ' || p_user;
   EXCEPTION
      WHEN e_no_obj
      THEN
      raise_application_error( -20004, 'Some application objects do not exist.' );
   END grant_transcend_app_privs;

   PROCEDURE build_sys_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      create_user( p_user => p_schema, p_tablespace => p_tablespace );
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- create all the tables
      BEGIN
         EXECUTE IMMEDIATE q'|CREATE TABLE repositories
	 ( 
	   repository_name    VARCHAR2(30) NOT NULL,
	   version	      NUMBER,
	   created_user	      VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	      DATE DEFAULT SYSDATE NOT NULL,
	   modified_user      VARCHAR2(30),
	   modified_dt	      DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE repositories ADD 
	 (
           CONSTRAINT repositories_pk
           PRIMARY KEY
	   (repository_name)
	   USING INDEX
	 )|';

      EXCEPTION
         WHEN e_tab_exists
         THEN
            NULL;
      END;


      BEGIN
         EXECUTE IMMEDIATE q'|CREATE TABLE applications
	 ( 
	   application_name   VARCHAR2(30) NOT NULL,
	   repository_name    VARCHAR2(30) NOT NULL,
	   version 	      NUMBER,
	   created_user	      VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	      DATE DEFAULT SYSDATE NOT NULL,
	   modified_user      VARCHAR2(30),
	   modified_dt	      DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE applications ADD 
	 (
	   CONSTRAINT applications_pk
	   PRIMARY KEY
	   (application_name)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE applications ADD 
	 (
	   CONSTRAINT applications_fk1
	   FOREIGN KEY (repository_name)
	   REFERENCES repositories  
	   ( repository_name )
	 )|';

      EXCEPTION
         WHEN e_tab_exists
         THEN
            NULL;
      END;


      BEGIN
         EXECUTE IMMEDIATE q'|CREATE TABLE users
	 ( 
	   user_name          VARCHAR2(30) NOT NULL,
	   application_name   VARCHAR2(30) NOT NULL,
	   repository_name    VARCHAR2(30) NOT NULL,
	   version 	      NUMBER,
	   created_user	      VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	      DATE DEFAULT SYSDATE NOT NULL,
	   modified_user      VARCHAR2(30),
	   modified_dt	      DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_pk
	   PRIMARY KEY
	   (user_name)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_fk1
	   FOREIGN KEY (repository_name)
	   REFERENCES repositories  
	   ( repository_name )
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE users ADD 
	 (
	   CONSTRAINT users_fk2
	   FOREIGN KEY (application_name)
	   REFERENCES applications 
	   ( application_name )
	 )|';

      EXCEPTION
         WHEN e_tab_exists
         THEN
            NULL;
      END;

      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      -- set CURRENT_SCHEMA back to where it started
      reset_current_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set CURRENT_SCHEMA back to where it started
         reset_current_schema;
         RAISE;
   END build_sys_repo;

   PROCEDURE drop_evolve_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
      l_sel_role	VARCHAR2(30) := p_schema || '_sel';
      l_adm_role 	VARCHAR2(30) := p_schema || '_adm';
   BEGIN
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- drop repository tables
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
      
      -- create a sequence for concurrent ids
      BEGIN
         EXECUTE IMMEDIATE q'|DROP SEQUENCE concurrent_id_seq|';
      EXCEPTION
	 WHEN e_no_seq
	 THEN
	 NULL;
      END;
      
      -- drop repository roles
      BEGIN
         EXECUTE IMMEDIATE 'DROP role ' || l_sel_role;
      EXCEPTION
         WHEN e_no_grantee
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP role ' || l_adm_role;
      EXCEPTION
         WHEN e_no_grantee
         THEN
         NULL;
      END;

      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END drop_evolve_repo;

   PROCEDURE build_evolve_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop         BOOLEAN DEFAULT FALSE
   )
   IS
      l_sel_role	VARCHAR2(30) := upper(p_schema)||'_SEL';
      l_adm_role	VARCHAR2(30) := upper(p_schema)||'_ADM';
   BEGIN
      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      create_user( p_user => p_schema, p_tablespace => p_tablespace );
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- this will drop all the tables before beginning
      -- also drops the repository roles
      IF p_drop
      THEN
	drop_evolve_repo( p_schema => p_schema );
      END IF;

      -- create the repository roles      
      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || l_sel_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || l_adm_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;

      BEGIN
	 
	 EXECUTE IMMEDIATE q'|CREATE SEQUENCE concurrent_id_seq|';

         -- DIR_LIST table
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE dir_list
	 ( 
	   filename VARCHAR2(255),
	   file_dt date,
	   file_size NUMBER
	 )
	 ON COMMIT DELETE ROWS|';

         -- COUNT_TABLE table
         EXECUTE IMMEDIATE q'|CREATE TABLE count_table
	 (
	   entry_ts       TIMESTAMP DEFAULT systimestamp NOT null,
	   client_info    VARCHAR2(64),
	   module         VARCHAR2(48),
	   action         VARCHAR2(32),
	   runmode 	  VARCHAR2(10) NOT NULL,
	   session_id     NUMBER NOT null,
	   row_cnt        NUMBER NOT null
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE count_table ADD 
	 (
	   CONSTRAINT count_table_pk
	   PRIMARY KEY
	   (session_id,entry_ts)
	   USING INDEX
	 )|';

         -- ERROR_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE error_conf
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

         EXECUTE IMMEDIATE q'|ALTER TABLE error_conf ADD 
	 (
	   CONSTRAINT error_conf_pk
	   PRIMARY KEY
	   (name)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE error_conf ADD 
	 (
	   CONSTRAINT error_conf_uk1
	   unique
	   (code)
	   USING INDEX
	 )|';

         -- LOG_TABLE table
         EXECUTE IMMEDIATE q'|CREATE TABLE log_table
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
         EXECUTE IMMEDIATE q'|CREATE TABLE logging_conf
	 ( 
	   logging_level    NUMBER not NULL,
	   debug_level 	  NUMBER NOT NULL,
	   module 	  VARCHAR2(48) NOT NULL,
	   created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt       DATE DEFAULT SYSDATE NOT NULL,
	   modified_user    VARCHAR2(30),
	   modified_dt	  DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE logging_conf ADD 
	 (
	   CONSTRAINT logging_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

         -- NOTIFICATION_EVENTS table
         EXECUTE IMMEDIATE q'|CREATE TABLE notification_events
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

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_events ADD
	 (
	   CONSTRAINT notification_events_pk
	   PRIMARY KEY
	   ( action, module )
	   USING INDEX
	 )|';

         -- NOTIFICATION_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE notification_conf
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

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD
	 (
	   CONSTRAINT notification_conf_pk
	   PRIMARY KEY
	   ( label,module,action )
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD 
	 (
	   CONSTRAINT notification_conf_fk1
	   FOREIGN KEY ( module, action )
	   REFERENCES notification_events
	   ( module, action )
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck1 CHECK (module=lower(module))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck2 CHECK (action=lower(action))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck3 CHECK (method=lower(method))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck4 CHECK (enabled=lower(enabled))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck5 CHECK (required=lower(required))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck6 CHECK (sender=lower(sender))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck7 CHECK (recipients=lower(recipients))|';

         -- REGISTRATION_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE registration_conf
	 ( 
	   registration  	     VARCHAR2(10) NOT NULL,
	   module 	     VARCHAR2(48),
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
	   modified_dt	     DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE registration_conf ADD 
	 (
	   CONSTRAINT registration_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck1 CHECK (module=lower(module))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE registration_conf ADD CONSTRAINT registration_conf_ck2 CHECK (registration=lower(registration))|';

         -- RUNMODE_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE runmode_conf
	 ( 
	   default_runmode  VARCHAR2(10) not NULL,
	   module 	  VARCHAR2(48),
	   created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt       DATE DEFAULT SYSDATE NOT NULL,
	   modified_user    VARCHAR2(30),
	   modified_dt	  DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE runmode_conf ADD 
	 (
	   CONSTRAINT runmode_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE runmode_conf ADD CONSTRAINT runmode_conf_ck1 CHECK (module=lower(module))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE runmode_conf ADD CONSTRAINT runmode_conf_ck2 CHECK (default_runmode=lower(default_runmode))|';

         -- PARAMETER_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE parameter_conf
	 ( 
	   name		VARCHAR2(40) NOT NULL,
	   value 		VARCHAR2(40),
	   module 	VARCHAR2(48) NOT NULL,
	   created_user   VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  VARCHAR2(30),
	   modified_dt    DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE parameter_conf ADD 
	 (
	   CONSTRAINT parameter_conf_pk
	   PRIMARY KEY
	   (name,module)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck1 CHECK (lower(value) <> 'default')|';

         EXECUTE IMMEDIATE q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck2 CHECK (value=lower(value))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE parameter_conf ADD CONSTRAINT parameter_conf_ck3 CHECK (module=lower(module))|';
	 
	 -- grant select privileges to the select role
	 grant_evolve_rep_privs( p_grantee=> l_sel_role, p_mode => 'select');

	 -- grant all privileges to the admin role
	 grant_evolve_rep_privs( p_grantee=> l_adm_role, p_mode => 'admin');
	 
	 -- write the audit record for creating or modifying the repository
	 -- doe this as an EXECUTE IMMEDIATE because the package won't compile otherwise
	 -- that's because the package itself creates the table
         EXECUTE IMMEDIATE q'|UPDATE tdsys.repositories
	 SET modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	 modified_dt = SYSDATE
	 WHERE repository_name=upper(:b_schema)|'
                     USING p_schema;

         IF SQL%ROWCOUNT = 0
         THEN
            EXECUTE IMMEDIATE q'|INSERT INTO tdsys.repositories
	    ( repository_name, version)
	    VALUES
	    ( upper(:b_schema),:b_version)|'
            USING p_schema, td_version;
         END IF;
      EXCEPTION
         WHEN e_tab_exists
         THEN
            RAISE e_repo_obj_exists;
      END;

      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END build_evolve_repo;

   PROCEDURE drop_transcend_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
      PRAGMA EXCEPTION_INIT( e_stat_tab_exists, -20002 );
   BEGIN
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- this will drop all the tables before beginning
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

      EXCEPTION
         WHEN e_tab_exists OR e_stat_tab_exists
         THEN
            RAISE e_repo_obj_exists;
      END;

      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END drop_transcend_repo;
   
   PROCEDURE build_transcend_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop         BOOLEAN DEFAULT FALSE
   )
   IS
      e_stat_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_stat_tab_exists, -20002 );
   BEGIN
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- this will drop all the tables before beginning
      IF p_drop
      THEN
	 -- drop the repository objects
	 drop_transcend_repo( p_schema => p_schema );
      END IF;

      BEGIN
         -- create the statitics table
         DBMS_STATS.create_stat_table( p_schema, 'OPT_STATS' );

         -- FILES_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE files_conf
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
	   match_parameter     VARCHAR2(10),
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

         EXECUTE IMMEDIATE q'|ALTER TABLE files_conf ADD 
	 (
	   CONSTRAINT files_conf_pk
	   PRIMARY KEY
	   (file_label, file_group)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE files_conf ADD 
	 (
	   CONSTRAINT files_conf_ck1
	   CHECK (source_policy IN ('oldest','newest','all','fail',NULL))
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE files_conf ADD
	   CONSTRAINT files_conf_ck2
	   CHECK (file_type = case when source_directory is null or source_regexp is null then 'extract' ELSE file_type END )|';

         -- FILES_DETAIL table
         EXECUTE IMMEDIATE q'|CREATE TABLE files_detail
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

         EXECUTE IMMEDIATE q'|ALTER TABLE files_detail ADD 
	 (
	   CONSTRAINT file_detail_pk
	   PRIMARY KEY
	   (file_detail_id)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE files_detail ADD 
	 (
	   CONSTRAINT file_detail_fk1
	   FOREIGN KEY ( file_label, file_group )
	   REFERENCES files_conf
	   ( file_label, file_group )
	 )|';

         EXECUTE IMMEDIATE q'|CREATE SEQUENCE files_detail_seq|';

         -- FILES_OBJ_DETAIL table
         EXECUTE IMMEDIATE q'|CREATE TABLE files_obj_detail
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

         EXECUTE IMMEDIATE q'|ALTER TABLE files_obj_detail ADD 
	 (
	   CONSTRAINT files_obj_detail_pk
	   PRIMARY KEY
	   (file_obj_detail_id)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|CREATE SEQUENCE files_obj_detail_seq|';

         -- TD_PART_GTT table
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE td_part_gtt
	 ( 
	   table_owner VARCHAR2(30),
	   table_name VARCHAR2(30),
	   partition_name VARCHAR2(30),
	   partition_position NUMBER
	 )
	 ON COMMIT DELETE ROWS|';

         -- TD_BUILD_IDX_GTT
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE td_build_idx_gtt
	 ( 
	   rename_ddl 	      VARCHAR2(4000),
	   rename_msg 	      VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';

         -- TD_BUILD_CON_GTT
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE td_build_con_gtt
	 ( 
	   rename_ddl 	      VARCHAR2(4000),
	   rename_msg 	      VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';

         -- TD_CON_MAINT_GTT
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE td_con_maint_gtt
	 ( 
	   disable_ddl 	    VARCHAR2(4000),
	   disable_msg 	    VARCHAR2(4000),
	   enable_ddl 	    VARCHAR2(4000),
	   enable_msg 	    VARCHAR2(4000),
	   order_seq	    NUMBER
	 )
	 ON COMMIT DELETE ROWS|';

         -- COLUMN_TYPE_LIST table
         EXECUTE IMMEDIATE q'|CREATE TABLE column_type_list
	 ( 
	   column_type	VARCHAR2(30) NOT NULL,
	   created_user	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE column_type_list ADD 
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
         EXECUTE IMMEDIATE q'|CREATE TABLE mapping_conf
	 ( 
	   mapping_name		VARCHAR2(40),
	   mapping_type		VARCHAR2(10),
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

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD 
	 (
	   CONSTRAINT mapping_conf_pk
	   PRIMARY KEY
	   ( mapping_name )
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck1 CHECK (mapping_name=lower(mapping_name))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck2 CHECK (manage_indexes in ('yes','no'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck3 CHECK (manage_constraints in ('yes','no'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck4 CHECK (concurrent in ('yes','no'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck5 CHECK (replace_method in ('exchange','rename'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck6 CHECK (replace_method = case when table_owner <> source_owner and mapping_type = 'table' then 'exchange' else replace_method end )|';
	 
         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck7 CHECK (mapping_type in ('dimension','table'))|';

         -- DIMENSION_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE dimension_conf
	 ( 
	   table_owner		VARCHAR2(30) NOT NULL,
	   table_name		VARCHAR2(30) NOT NULL,
	   sequence_owner  	VARCHAR2(30) NOT NULL,
	   sequence_name  	VARCHAR2(30) NOT NULL,
	   staging_owner	VARCHAR2(30) DEFAULT NULL,
	   staging_table	VARCHAR2(30) DEFAULT NULL,
	   default_scd_type	NUMBER(1,0) DEFAULT 2 NOT NULL,
	   direct_load		VARCHAR2(3) DEFAULT 'yes' NOT NULL,
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

         EXECUTE IMMEDIATE q'|ALTER TABLE dimension_conf ADD 
	 (
	   CONSTRAINT dimension_conf_pk
	   PRIMARY KEY
	   ( table_owner, table_name )
	   USING INDEX
	 )|';
	 	 
         -- COLUMN_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE column_conf
	 ( 
	   table_owner	VARCHAR2(30) NOT NULL,
	   table_name	VARCHAR2(30) NOT NULL,
	   column_name	VARCHAR2(30) NOT NULL,
	   column_type	VARCHAR2(30) NOT NULL,
	   created_user	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	VARCHAR2(30),
	   modified_dt    	DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_pk
	   PRIMARY KEY
	   ( table_owner, table_name, column_name )
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_fk1
	   FOREIGN KEY ( column_type )
	   REFERENCES column_type_list
	   ( column_type )
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE column_conf ADD 
	 (
	   CONSTRAINT column_conf_fk2
	   FOREIGN KEY ( table_owner, table_name )
	   REFERENCES dimension_conf  
	   ( table_owner, table_name )
	 )|';

	 -- grant select privileges to the select role
	 grant_transcend_rep_privs( p_grantee=> l_sel_role, p_mode => 'select');

	 -- grant all privileges to the admin role
	 grant_transcend_rep_privs( p_grantee=> l_adm_role, p_mode => 'admin');

      EXCEPTION
         WHEN e_tab_exists OR e_stat_tab_exists
         THEN
            RAISE e_repo_obj_exists;
      END;

      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      -- set current_schema back to where it started
      reset_current_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END build_transcend_repo;

   PROCEDURE build_evolve_rep_syns( p_user VARCHAR2, p_schema VARCHAR2 )
   IS
   BEGIN
      -- create TDSYS synonyms
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.REPOSITORIES for TDSYS.REPOSITORIES';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.APPLICATIONS for TDSYS.APPLICATIONS';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.USERS for TDSYS.USERS';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      -- create the repository synonyms
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.COUNT_TABLE for ' || p_schema || '.COUNT_TABLE';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.DIR_LIST for ' || p_schema || '.DIR_LIST';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.ERROR_CONF for ' || p_schema || '.ERROR_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOGGING_CONF for ' || p_schema
                           || '.LOGGING_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_TABLE for ' || p_schema || '.LOG_TABLE';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
      
      -- sequences
      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.CONCURRENT_ID_SEQ for '
                           || p_schema
                           || '.CONCURRENT_ID_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;


      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.NOTIFICATION_CONF for '
                           || p_schema
                           || '.NOTIFICATION_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.NOTIFICATION_EVENTS for '
                           || p_schema
                           || '.NOTIFICATION_EVENTS';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.REGISTRATION_CONF for '
                           || p_schema
                           || '.REGISTRATION_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.RUNMODE_CONF for ' || p_schema
                           || '.RUNMODE_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.PARAMETER_CONF for '
                           || p_schema
                           || '.PARAMETER_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
   END build_evolve_rep_syns;

   PROCEDURE build_evolve_app_syns( p_user VARCHAR2, p_schema VARCHAR2 )
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TD_ADM for ' || p_schema || '.TD_ADM';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.APP_OT for ' || p_schema || '.APP_OT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.EVOLVE_OT for ' || p_schema || '.EVOLVE_OT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.SPLIT_OT for ' || p_schema || '.SPLIT_OT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      -- packages and functions
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.STRAGG for ' || p_schema || '.STRAGG';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TD_INST for ' || p_schema || '.TD_INST';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.EVOLVE for ' || p_schema || '.EVOLVE';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.EVOLVE_ADM for ' || p_schema || '.EVOLVE_ADM';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

   END build_evolve_app_syns;

   PROCEDURE build_transcend_rep_syns( p_user VARCHAR2, p_schema VARCHAR2 )
   IS
   BEGIN
      -- create the synonyms
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.FILES_CONF for ' || p_schema || '.FILES_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.FILES_DETAIL for ' || p_schema
                           || '.FILES_DETAIL';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILES_DETAIL_SEQ for '
                           || p_schema
                           || '.FILES_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILES_OBJ_DETAIL for '
                           || p_schema
                           || '.FILES_OBJ_DETAIL';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILES_OBJ_DETAIL_SEQ for '
                           || p_schema
                           || '.FILES_OBJ_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TD_PART_GTT for ' || p_schema || '.TD_PART_GTT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.TD_BUILD_IDX_GTT for '
                           || p_schema
                           || '.TD_BUILD_IDX_GTT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.TD_BUILD_CON_GTT for '
                           || p_schema
                           || '.TD_BUILD_CON_GTT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.TD_CON_MAINT_GTT for '
                           || p_schema
                           || '.TD_CON_MAINT_GTT';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.COLUMN_CONF for ' || p_schema || '.COLUMN_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.DIMENSION_CONF for '
                           || p_schema
                           || '.DIMENSION_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.MAPPING_CONF for ' || p_schema
                           || '.MAPPING_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.COLUMN_TYPE_LIST for '
                           || p_schema
                           || '.COLUMN_TYPE_LIST';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.OPT_STATS for ' || p_schema || '.OPT_STATS';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      -- create the synonyms for the sequences in the repository
      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILES_DETAIL_SEQ for '
                           || p_schema
                           || '.FILES_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILES_OBJ_DETAIL_SEQ for '
                           || p_schema
                           || '.FILES_OBJ_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
   END build_transcend_rep_syns;

   PROCEDURE build_transcend_app_syns( p_user VARCHAR2, p_schema VARCHAR2 )
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TRANS_ETL for ' || p_schema || '.TRANS_ETL';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TRANS_FILES for ' || p_schema || '.TRANS_FILES';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TRANS_ADM for ' || p_schema || '.TRANS_ADM';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
   END build_transcend_app_syns;

   PROCEDURE grant_evolve_sys_privs( p_schema VARCHAR2 DEFAULT DEFAULT_REPOSITORY, p_drop BOOLEAN DEFAULT FALSE )
   IS
      l_sys_role      VARCHAR2( 30 ) := p_schema || '_sys';
      l_java_role     VARCHAR2( 30 ) := p_schema || '_java';
      e_no_role       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
      e_ins_privs        EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_ins_privs, -1031 );
   BEGIN
      -- this will drop the roles before beginning
      IF p_drop
      THEN
         BEGIN
            EXECUTE IMMEDIATE 'DROP role ' || l_sys_role;
         EXCEPTION
            WHEN e_no_role
            THEN
               NULL;
         END;

         BEGIN
            EXECUTE IMMEDIATE 'DROP role ' || l_java_role;
         EXCEPTION
            WHEN e_no_role
            THEN
               NULL;
         END;
      END IF;

      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || l_sys_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || l_java_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;

      -- for each system privilege, grant it to the application owner and the _SYS role
      EXECUTE IMMEDIATE 'GRANT CONNECT TO ' || l_sys_role;

      EXECUTE IMMEDIATE 'GRANT CONNECT TO ' || p_schema;

      EXECUTE IMMEDIATE 'GRANT RESOURCE TO ' || l_sys_role;

      EXECUTE IMMEDIATE 'GRANT RESOURCE TO ' || p_schema;

      EXECUTE IMMEDIATE 'GRANT ALTER SESSION TO ' || l_sys_role;

      EXECUTE IMMEDIATE 'GRANT ALTER SESSION TO ' || p_schema;

      EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO ' || l_sys_role;

      EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO ' || p_schema;

      -- grant permissions on UTL_MAIL
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.utl_mail TO ' || l_sys_role;

         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.utl_mail TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	   dbms_output.put_line( 'The installing user cannot see package UTL_MAIL. EXECUTE on UTL_MAIL needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
	 WHEN e_ins_privs
	 THEN
	   dbms_output.put_line( 'The installing user cannot grant execute on UTL_MAIL. EXECUTE on UTL_MAIL needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
      END;

      -- grant permissions on DBMS_LOCK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_lock TO ' || l_sys_role;

         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_lock TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	   dbms_output.put_line( 'The installing user cannot see package DBMS_LOCK. EXECUTE on DBMS_LOCK needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
	 WHEN e_ins_privs
	 THEN
	   dbms_output.put_line( 'The installing user cannot grant execute on DBMS_LOCK. EXECUTE on DBMS_LOCK needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
      END;

      -- grant permissions on DBMS_FLASHBACK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_flashback TO ' || l_sys_role;

         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_flashback TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	   dbms_output.put_line( 'The installing user cannot see package DBMS_FLASHBACK. EXECUTE on DBMS_FLASHBACK needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
	 WHEN e_ins_privs
	 THEN
	   dbms_output.put_line( 'The installing user cannot grant execute on DBMS_FLASHBACK. EXECUTE on DBMS_FLASHBACK needs to be granted to user '||p_schema||' and role '||l_sys_role||'.' );
      END;

      -- grant java specific privilege to the _JAVA role
      DBMS_JAVA.set_output( 1000000 );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
      DBMS_JAVA.grant_permission( UPPER( l_java_role ), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor', '' );

      -- grant the _JAVA role to the app owner and the _APP role
      EXECUTE IMMEDIATE 'GRANT ' || l_java_role || ' TO ' || l_sys_role;

      EXECUTE IMMEDIATE 'GRANT ' || l_java_role || ' TO ' || p_schema;
   EXCEPTION
      WHEN OTHERS
      THEN
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END grant_evolve_sys_privs;

   PROCEDURE grant_transcend_sys_privs( p_schema VARCHAR2 DEFAULT DEFAULT_REPOSITORY )
   IS
      l_sys_role      VARCHAR2( 30 ) := p_schema || '_sys';
      l_java_role     VARCHAR2( 30 ) := p_schema || '_java';
      e_no_role       EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
   BEGIN
      BEGIN
         -- for each system privilege, grant it to the application owner and the _SYS role
         EXECUTE IMMEDIATE 'GRANT ALTER ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT INSERT ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT SELECT ANY dictionary TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT SELECT ANY SEQUENCE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT UPDATE ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT DELETE ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT ALTER ANY INDEX TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT CREATE ANY INDEX TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT DROP ANY INDEX TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT DROP ANY TABLE TO ' || p_schema || '_sys';

         EXECUTE IMMEDIATE 'GRANT ANALYZE ANY TO ' || p_schema || '_sys';
      EXCEPTION
         WHEN e_no_obj
         THEN
            raise_application_error( -20004, 'Some repository objects do not exist.' );
      END;
   END grant_transcend_sys_privs;

   PROCEDURE build_evolve_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop         BOOLEAN DEFAULT FALSE
   )
   IS
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user => p_schema );
      -- two packages that are needed

      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );

      -- drop all the code objects if they exist
      drop_evolve_app;
      
      -- create grants to the application owner to all the tables in the repository
      grant_evolve_rep_privs( p_grantee => p_schema );
      -- set the CURRENT_SCHEMA back
      reset_current_schema;
      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );
      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user => p_schema, p_schema => p_repository );
      -- grant application privileges to the roles
      grant_evolve_sys_privs( p_schema => p_schema );
      -- create the dbms_scheduler program
      create_scheduler_metadata;

      -- write application tracking record
      EXECUTE IMMEDIATE q'|UPDATE tdsys.applications
      SET repository_name = upper(:b_rep_schema),
      modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
      modified_dt = SYSDATE
      WHERE application_name=upper(:b_app_schema)|'
                  USING p_repository, p_schema;

      IF SQL%ROWCOUNT = 0
      THEN
         EXECUTE IMMEDIATE q'|INSERT INTO tdsys.applications
	 ( application_name,
	   repository_name,
	   version )
	 VALUES
	 ( upper(:b_app_schema),
	   upper(:b_rep_schema),
	   :b_version )|'
         USING p_schema, p_repository, td_version;
      END IF;

      DBMS_OUTPUT.put_line(    ' The CURRENT_SCHEMA is set to '
                            || SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' )
                            || ' in preparation for installing application'
                          );
   END build_evolve_app;

   PROCEDURE build_transcend_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop         BOOLEAN DEFAULT FALSE
   )
   IS
   BEGIN
      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );

      -- drop all the transcend application objects in order to make sure they can be recreated
      drop_transcend_app;

      -- create grants to the application owner to all the tables in the repository
      grant_transcend_rep_privs( p_user => p_schema );
      -- set the CURRENT_SCHEMA back
      reset_current_schema;
      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );
      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user => p_schema, p_schema => p_repository );
      -- grant application privileges to the roles
      grant_transcend_sys_privs( p_schema => p_schema );
   END build_transcend_app;

   PROCEDURE drop_evolve_app
   IS
   BEGIN
      
      -- this type is created first as it's needed for the TD_CORE
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE split_ot';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;


      -- td_core package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package td_core';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;

      -- STRAGG function
      BEGIN
         EXECUTE IMMEDIATE 'DROP package string_agg_ot';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP function stragg';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- java stored procedures
      BEGIN
         EXECUTE IMMEDIATE 'DROP java source TdCore';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- td_inst package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package td_inst';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- evolve package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package evolve';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- types need to be dropped in a specific order
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE notification_ot';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE evolve_ot';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE app_ot';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

      -- utilities package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package td_utils';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;
      
      -- evolve callable packages
      BEGIN
         EXECUTE IMMEDIATE 'DROP package evolve';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package evolve_adm';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

   END drop_evolve_app;

   PROCEDURE drop_transcend_app
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE dimension_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE mapping_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE feed_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE extract_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE file_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP package td_dbutils';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package trans_adm';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package trans_etl';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package trans_files';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

   END drop_transcend_app;

   PROCEDURE create_evolve_user(
      p_user          VARCHAR2,
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository    VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user => p_user );
      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user => p_user, p_schema => p_repository );
      -- create the synonyms to the application
      build_evolve_app_syns( p_user => p_user, p_schema => p_application );

      -- grant execute on the framework to the new user
      grant_evolve_app_privs( p_user=> p_user, p_schema => p_application );

      EXECUTE IMMEDIATE 'grant ' || p_repository || '_adm to ' || p_user;

      -- write audit record for creating or modifying a user record
      -- use EXECUTE IMMEDIATE because the table does not exist when this package is created
      EXECUTE IMMEDIATE q'|UPDATE tdsys.users
      SET application_name = upper(:b_app_schema),
      repository_name = upper(:b_rep_schema),
      modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
      modified_dt = SYSDATE
      WHERE user_name=upper(:b_user)|'
                  USING p_application, p_repository, p_user;

      IF SQL%ROWCOUNT = 0
      THEN
         EXECUTE IMMEDIATE q'|INSERT INTO tdsys.users
	 ( user_name,
	   application_name,
	   repository_name,
	   version )
	 VALUES
	 ( upper(:b_user),
	   upper(:b_app_schema),
	   upper(:b_rep_schema),
	   :b_version )|'
         USING p_user, p_application, p_repository, td_version;
      END IF;
   END create_evolve_user;

   PROCEDURE create_transcend_user(
      p_user          VARCHAR2,
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository    VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- create the user if it doesn't already exist
      create_user( p_user => p_user );

      EXECUTE IMMEDIATE 'grant select_catalog_role to ' || p_user;

      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user => p_user, p_schema => p_repository );
      -- create the synonyms to the application
      build_transcend_app_syns( p_user => p_user, p_schema => p_application );
      
      -- grant execute on the framework to the new user
      grant_transcend_app_privs( p_user=> p_user, p_schema => p_application );

   END create_transcend_user;
END td_adm;
/

SHOW errors