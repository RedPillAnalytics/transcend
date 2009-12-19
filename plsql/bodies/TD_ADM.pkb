CREATE OR REPLACE PACKAGE BODY td_adm
IS
   g_user             dba_users.username%TYPE;
   g_tablespace       dba_users.default_tablespace%TYPE;
   g_current_schema   dba_users.username%TYPE := SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' );

   -- exceptions used over and over agaoin
   -- define them only once
   e_no_user      EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_user, -1435 );
   e_obj_exists   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_obj_exists, -955 );
   e_role_exists  EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_role_exists, -1921 );
   e_no_grantee   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_grantee, -1919 );
   e_no_obj       EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_obj, -4043 );
   e_tab_exists   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_tab_exists, -955 );
   e_no_tab       EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tab, -942 );
   e_no_seq       EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_seq, -2289 );
   e_same_name    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_same_name, -1471 );
   e_ins_privs    EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_ins_privs, -1031 );
   e_no_role      EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_role, -1919 );
   e_col_exists   EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_col_exists, -1430 );
   e_already_null EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_already_null, -1451 );

   FUNCTION get_product_name(
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
      RETURN VARCHAR2
   IS
      l_product       tdsys.applications.product%type;
   BEGIN

      -- select in the product name for this application
      BEGIN
         SELECT product
           INTO l_product
           FROM tdsys.applications
          WHERE lower(application_name) = lower(p_application);
      EXCEPTION
         -- make sure there is an application by this name
         WHEN no_data_found
         THEN 
            raise_application_error( -20009, 'Specified APPLICATION does not exist');
      END;
      
      RETURN l_product;
   END get_product_name;

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
   
   PROCEDURE reset_current_schema
   IS
   BEGIN
      EXECUTE IMMEDIATE 'alter session set current_schema=' || g_current_schema;
   END reset_current_schema;

   PROCEDURE set_default_tablespace( p_user VARCHAR2, p_tablespace VARCHAR2 )
   IS
   BEGIN
      -- find out if the user exists
      -- also get the current default tablespace of the user
      BEGIN
         SELECT default_tablespace
           INTO g_tablespace
           FROM dba_users
          WHERE username = UPPER( p_user );
      EXCEPTION
      WHEN no_data_found
      THEN
         RAISE unknown_user;
      END;

      IF p_tablespace IS NOT NULL
      THEN
         g_user := p_user;

         EXECUTE IMMEDIATE 'alter user ' || p_user || ' default tablespace ' || p_tablespace;
      END IF;	 


   END set_default_tablespace;

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
   
   PROCEDURE create_user( p_user VARCHAR2 DEFAULT DEFAULT_REPOSITORY, p_tablespace VARCHAR2 DEFAULT NULL )
   IS
      l_user           all_users.username%TYPE;
      l_def_tbs        database_properties.property_value%TYPE;
      l_ddl	       LONG;
   BEGIN
      -- get the database default tablespace
      SELECT property_value
        INTO l_def_tbs
        FROM database_properties
       WHERE property_name = 'DEFAULT_PERMANENT_TABLESPACE';
      
      -- set the default tablespace for the user
      BEGIN
         set_default_tablespace( p_user => p_user, p_tablespace => p_tablespace );
      EXCEPTION
         WHEN unknown_user
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
                                  || ' This is the job class Evolve calls by default when the Oracle scheduler is used for concurrent processing'
         );
   END create_scheduler_metadata;

   PROCEDURE drop_evolve_app ( p_schema VARCHAR2 )
   IS
   BEGIN
      
      -- drop some of the old roles used in earlier versions
      BEGIN
         EXECUTE IMMEDIATE 'DROP ROLE '||p_schema||'_java';
      EXCEPTION
         when e_no_role
         THEN
         NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP ROLE '||p_schema||'_sys';
      EXCEPTION
         when e_no_role
         THEN
         NULL;
      END;

      
      -- this type is created first as it's needed for the TD_CORE
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.split_ot';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;


      -- td_core package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.td_core';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;

      -- STRAGG function
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.string_agg_ot';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP function '||p_schema||'.stragg';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- java stored procedures
      BEGIN
         EXECUTE IMMEDIATE 'DROP java source '||p_schema||'.TdUtils';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- td_inst package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.td_inst';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- evolve package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.evolve';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- drop some older evolve packages just in case
      -- these no longer exist in current versions
      -- evolve package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.evolve_app';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      -- evolve package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.evolve_log';
      EXCEPTION
         when e_no_obj
         THEN
         NULL;
      END;
      
      -- types need to be dropped in a specific order
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.notification_ot';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.evolve_ot';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

      -- utilities package
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.td_utils';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;
      
      -- evolve callable packages
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.evolve';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.evolve_adm';
      EXCEPTION
         when e_no_obj
         THEN
            NULL;
      END;

   END drop_evolve_app;

   PROCEDURE drop_transcend_app ( p_schema VARCHAR2 )
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.dimension_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.mapping_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.feed_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.extract_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP TYPE '||p_schema||'.file_ot';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.td_dbutils';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.trans_adm';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.trans_etl';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP package '||p_schema||'.trans_files';
      EXCEPTION
         WHEN e_no_obj
         THEN
            NULL;
      END;

   END drop_transcend_app;

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
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON RESULTS_TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON DIR_LIST TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON MODULE_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON LOG_TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON NOTIFICATION_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON NOTIFICATION_EVENT TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON PARAMETER_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON ERROR_CONF TO ' || p_grantee;
         
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COMMAND_CONF TO ' || p_grantee;
         
	 
         -- sequences
         EXECUTE IMMEDIATE 'grant select on CONCURRENT_ID_SEQ to ' || p_grantee;
         
         -- views
         EXECUTE IMMEDIATE 'grant select on log to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_runtime to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_runtime_today to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_today to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_week to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_my_session to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_runtime_my_session to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_debug to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_debug_today to ' || p_grantee;

         EXECUTE IMMEDIATE 'grant select on log_debug_my_session to ' || p_grantee;


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
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.EVOLVE_OT to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.SPLIT_OT to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.STRAGG to ' || p_user;

      -- packages
      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TD_INST to ' || p_user;

      EXECUTE IMMEDIATE 'grant execute on '||p_schema||'.TD_CORE to ' || p_user;
      
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
      p_mode	  VARCHAR2 DEFAULT 'admin')
   AS
      l_grant VARCHAR2(100);
   BEGIN

      -- if p_mode is 'select', then only grant select privilege
      -- if it's 'admin', then grant all privileges
      l_grant := CASE p_mode WHEN 'select' THEN 'SELECT' ELSE 'SELECT,UPDATE,INSERT,DELETE' END;
	 
      BEGIN
	 
	 -- tables
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILE_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILE_DETAIL TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON FILE_OBJECT_DETAIL TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON TD_PART_GTT TO ' || p_grantee;
	 
         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON DDL_QUEUE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON OPT_STATS TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON DIMENSION_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON MAPPING_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COLUMN_CONF TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT '||l_grant||' ON COLUMN_TYPE_LIST TO ' || p_grantee;
	 
	 -- sequence
         EXECUTE IMMEDIATE 'GRANT SELECT ON file_detail_seq TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT SELECT ON file_object_detail_seq TO ' || p_grantee;

      EXCEPTION
         WHEN e_no_grantee
         THEN
            raise_application_error( -20005,
                                     'The grantees ' || p_grantee || ' does not exist.'
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
         EXECUTE IMMEDIATE q'|DROP TABLE parameter_conf|';
      EXCEPTION
         WHEN e_no_tab
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE q'|DROP TABLE results_table|';
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
         EXECUTE IMMEDIATE q'|DROP TABLE command_conf|';
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
         EXECUTE IMMEDIATE q'|DROP TABLE module_conf|';
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
         EXECUTE IMMEDIATE q'|DROP TABLE notification_event|';
      EXCEPTION
         WHEN e_no_tab
         THEN
         NULL;
      END;

      -- log table views
      BEGIN
         EXECUTE IMMEDIATE 'drop view log';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_runtime';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_runtime_today';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_today';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_week';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_my_session';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_runtime_my_session';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_debug';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_debug_today';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'drop view log_debug_my_session';
      EXCEPTION
         WHEN e_no_tab
         THEN
            NULL;
      END;
      
      -- sequence for concurrent id's
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
	   file_size NUMBER,
	   create_ts TIMESTAMP DEFAULT systimestamp
	 )
	 ON COMMIT DELETE ROWS|';

         -- RESULTS_TABLE table
         EXECUTE IMMEDIATE q'|CREATE TABLE results_table
	 (
	   entry_ts       TIMESTAMP DEFAULT systimestamp NOT null,
	   client_info    VARCHAR2(64),
	   module         VARCHAR2(48),
	   action         VARCHAR2(32),
	   runmode 	  VARCHAR2(10) NOT NULL,
	   session_id     NUMBER NOT null,
           object_owner   VARCHAR2(30),
           object_name    VARCHAR2(30),
           dml_category   VARCHAR2(30),
	   row_count      NUMBER,
           duration       NUMBER
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE results_table ADD 
	 (
	   CONSTRAINT results_table_pk
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

         -- COMMAND_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE command_conf
	 ( 
           name                VARCHAR2(30) NOT NULL,
           value               VARCHAR2(30),
           path                VARCHAR2(200),
           flags               VARCHAR2(100),
	   created_user        VARCHAR2(30),
	   created_dt          DATE,
	   modified_user       VARCHAR2(30),
	   modified_dt         DATE,
	   description         VARCHAR2(100)
	 )|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE command_conf ADD 
	 (
	   CONSTRAINT command_conf_pk
	   PRIMARY KEY
	   (name)
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
	   logging_level NUMBER NOT NULL,
	   session_id NUMBER NOT NULL,
	   current_scn NUMBER NOT NULL,
	   instance_name VARCHAR2(30) NOT NULL,
	   machine VARCHAR2(100),
	   dbuser VARCHAR2(30),
	   osuser VARCHAR2(30),
	   code NUMBER NOT NULL,
	   call_stack VARCHAR2(1024),
	   back_trace VARCHAR2(1024),
	   batch_id number
	 )|';
         
         -- MODULE_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE module_conf
	 ( logging_level    NUMBER       NOT NULL,
	   debug_level 	    NUMBER       NOT NULL,
           default_runmode  VARCHAR2(10) NOT NULL,
           registration     VARCHAR2(10) NOT NULL,
	   module 	    VARCHAR2(48) NOT NULL,
	   created_user     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt       DATE DEFAULT SYSDATE NOT NULL,
	   modified_user    VARCHAR2(30),
	   modified_dt	    DATE
	 )|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE module_conf ADD 
	 (
	   CONSTRAINT module_conf_pk
	   PRIMARY KEY
	   (module)
	   USING INDEX
	 )|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE module_conf ADD CONSTRAINT module_conf_ck1 CHECK (module=lower(module))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE module_conf ADD CONSTRAINT module_conf_ck2 CHECK (default_runmode=lower(default_runmode))|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE module_conf ADD CONSTRAINT module_conf_ck3 CHECK (registration=lower(registration))|';
         
         -- NOTIFICATION_EVENT table
         EXECUTE IMMEDIATE q'|CREATE TABLE notification_event
	 ( 
           event_name          VARCHAR2(30)       NOT NULL,
           module              VARCHAR2(48)       NOT NULL,
           action              VARCHAR2(32)       NOT NULL,
           subject             VARCHAR2(100)      NOT NULL,
           message             VARCHAR2(2000)     NOT NULL,
           created_user        VARCHAR2(30)       DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
           created_dt          DATE               DEFAULT SYSDATE NOT NULL,
           modified_user       VARCHAR2(30),
           modified_dt         DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_event ADD
	 (
	   CONSTRAINT notification_event_pk
	   PRIMARY KEY
	   ( event_name )
	   USING INDEX
	 )|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE notification_event ADD
	 (
	   CONSTRAINT notification_event_uk1
	   UNIQUE
	   ( action, module )
	   USING INDEX
	 )|';

         -- NOTIFICATION_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE notification_conf
	 ( 
           label           VARCHAR2(40)     NOT NULL,
           event_name      VARCHAR2(30)     NOT NULL,
           method          VARCHAR2(20)     NOT NULL,
           enabled         VARCHAR2(3)      DEFAULT 'yes' NOT NULL,
           required        VARCHAR2(3)      DEFAULT 'no'  NOT NULL,
           sender          VARCHAR2(1024)   NOT NULL,
           recipients      VARCHAR2(2000)   NOT NULL,
           created_user    VARCHAR2(30)     DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
           created_dt      DATE             DEFAULT SYSDATE NOT NULL,
           modified_user   VARCHAR2(30),
           modified_dt     DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD
	 (
	   CONSTRAINT notification_conf_pk
	   PRIMARY KEY
	   ( label, event_name )
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD 
	 (
	   CONSTRAINT notification_conf_fk1
	   FOREIGN KEY ( event_name )
	   REFERENCES notification_event
	   ( event_name )
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck1 CHECK (event_name=lower(event_name))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck2 CHECK (method=lower(method))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck3 CHECK (enabled=lower(enabled))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck4 CHECK (required=lower(required))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck5 CHECK (sender=lower(sender))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck6 CHECK (recipients=lower(recipients))|';

         -- PARAMETER_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE parameter_conf
	 ( 
           name           VARCHAR2(40)   NOT NULL,
           value          VARCHAR2(40)   NOT NULL,
           module         VARCHAR2(48)   NOT NULL,
           created_user   VARCHAR2(30)   DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
           created_dt     DATE           DEFAULT SYSDATE NOT NULL,
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
         
         -- create log_table views
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';
         
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_runtime
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 WHERE runmode='runtime'
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_runtime_today
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 WHERE runmode='runtime'
         	   AND to_char(systimestamp, 'mmddyyyy') = to_char(entry_ts, 'mmddyyyy')
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';


         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_today
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 WHERE to_char(systimestamp, 'mmddyyyy') = to_char(entry_ts, 'mmddyyyy')
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';


         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_week
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
                 WHERE entry_ts > systimestamp - 7
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';
         
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_my_session
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM log_table
          WHERE session_id = sys_context('USERENV','SESSIONID')
          ORDER BY entry_ts |';
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_runtime_my_session
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM log_table
          WHERE session_id = sys_context('USERENV','SESSIONID')
            AND runmode='runtime'
          ORDER BY entry_ts |';
         
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_debug
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 WHERE runmode='debug'
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_debug_today
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM (SELECT client_info,
                        module,
                        action,
                        entry_ts,
                        msg,
                        call_stack,
                        back_trace,
                        session_id,
                        runmode,
                        current_scn,
                        instance_name,
                        service_name,
                        machine, 
                        dbuser, 
                        osuser, 
                        code, 
                        first_value(batch_id) OVER (partition BY session_id ORDER BY entry_ts) batch_id,
         	       MAX(entry_ts) OVER (partition BY session_id) last_entry_ts
         	  FROM log_table
         	 WHERE runmode='debug'
         	   AND to_char(systimestamp, 'mmddyyyy') = to_char(entry_ts, 'mmddyyyy')
         	 ORDER BY last_entry_ts,
         	       session_id,
         	       entry_ts) |';
         
         
         EXECUTE IMMEDIATE q'|CREATE OR REPLACE VIEW log_debug_my_session
         ( client_info,
           module,
           action,
           entry_ts,
           msg,
           call_stack,
           back_trace,
           batch_id,
           session_id,
           runmode,
           current_scn,
           instance_name,
           service_name,
           machine, 
           dbuser, 
           osuser, 
           code )
         AS 
         SELECT client_info,
                module,
                action,
                entry_ts,
                msg,
                call_stack,
                back_trace,
                batch_id,
                session_id,
                runmode,
                current_scn,
                instance_name,
                service_name,
                machine, 
                dbuser, 
                osuser, 
                code 
           FROM log_table
          WHERE session_id = sys_context('USERENV','SESSIONID')
            AND runmode='debug'
          ORDER BY entry_ts |';
	 
	 -- grant select privileges to the select role
	 grant_evolve_rep_privs( p_grantee=> l_sel_role, p_mode => 'select');

	 -- grant all privileges to the admin role
	 grant_evolve_rep_privs( p_grantee=> l_adm_role, p_mode => 'admin');
	 
	 -- write the audit record for creating or modifying the repository
	 -- doe this as an EXECUTE IMMEDIATE because the package won't compile otherwise
	 -- that's because the package itself creates the table
         UPDATE tdsys.repositories
	    SET modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
		modified_dt = SYSDATE,
		version = product_version,
		product = evolve_product
	  WHERE repository_name=upper( p_schema );

         IF SQL%ROWCOUNT = 0
         THEN
            INSERT INTO tdsys.repositories
		   ( repository_name, product, version)
		   VALUES
		   ( upper( p_schema ), evolve_product, product_version );
         END IF;
      EXCEPTION
         WHEN e_tab_exists
         THEN
            RAISE repo_obj_exists;
      END;

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
      e_stat_tab_exists EXCEPTION;
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
         EXECUTE IMMEDIATE q'|DROP TABLE file_object_detail|';
      EXCEPTION
         WHEN e_no_tab
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE q'|DROP TABLE file_detail|';
      EXCEPTION
         WHEN e_no_tab
         THEN
         NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE q'|DROP TABLE file_conf|';
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
         EXECUTE IMMEDIATE q'|DROP TABLE ddl_queue|';
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
         EXECUTE IMMEDIATE q'|DROP sequence file_detail_seq|';
      EXCEPTION
         WHEN e_no_seq
         THEN
         NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE q'|DROP sequence file_object_detail_seq|';
      EXCEPTION
         WHEN e_no_seq
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
   END drop_transcend_repo;
   
   PROCEDURE build_transcend_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop         BOOLEAN DEFAULT FALSE
   )
   IS
      e_stat_tab_exists   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_stat_tab_exists, -20002 );
   BEGIN

      -- this will drop all the tables before beginning
      IF p_drop
      THEN
	 -- drop the repository objects
	 drop_transcend_repo( p_schema => p_schema );
      END IF;

      BEGIN
         -- create the statitics table
         DBMS_STATS.create_stat_table( p_schema, 'OPT_STATS' );
         
         -- FILE_CONF table
         EXECUTE IMMEDIATE q'|CREATE TABLE file_conf
	 ( 
	   file_label	       VARCHAR2(100) 	NOT NULL,
	   file_group	       VARCHAR2(64) 	NOT NULL,
	   label_type	       VARCHAR2(7) 	NOT NULL,
	   object_owner	       VARCHAR2(30),
	   object_name	       VARCHAR2(30),
	   directory	       VARCHAR2(30)	NOT NULL,
	   filename	       VARCHAR2(50),		
	   work_directory      VARCHAR2(30),
	   min_bytes	       NUMBER,
	   max_bytes           NUMBER,
	   file_datestamp      VARCHAR2(30),
	   baseurl             VARCHAR2(500),
	   passphrase          VARCHAR2(100),
	   source_directory    VARCHAR2(50),
	   source_regexp       VARCHAR2(100),
	   match_parameter     VARCHAR2(10),
	   source_policy       VARCHAR2(10),
	   store_original_files  VARCHAR2(3),
   	   compress_method     VARCHAR2( 20 ),
   	   encrypt_method      VARCHAR2( 20 ),
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
	   modified_dt         DATE,
	   description         VARCHAR2(100)
	 )|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD 
	 (
	   CONSTRAINT file_conf_pk
	   PRIMARY KEY
	   (file_label)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD 
	 (
	   CONSTRAINT file_conf_ck1
	   CHECK (source_policy IN ('oldest','newest','all','fail',NULL))
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD
	   CONSTRAINT file_conf_ck2
	   CHECK (label_type = case when source_directory is null or source_regexp is null then 'extract' ELSE label_type END )|';
	 
	 
         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD
	   CONSTRAINT file_conf_ck3
	 CHECK ( 0 = CASE WHEN object_owner IS NULL AND object_name IS NOT NULL THEN 1 
		 WHEN object_owner IS NOT NULL AND object_name IS NULL THEN 1 ELSE 0 END )|';
	 	   
         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD 
	 (
	   CONSTRAINT file_conf_ck4
	   CHECK (store_original_files IN ('yes','no'))
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD 
	 (
	   CONSTRAINT file_conf_ck5
	   CHECK (compress_method IN ('extension_method','gzip_method','compress_method','bzip2_method','zip_method'))
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_conf ADD 
	 (
	   CONSTRAINT file_conf_ck6
	   CHECK (encrypt_method IN ('extension_method','gpg_method'))
	 )|';
	 
         -- FILE_DETAIL table
         EXECUTE IMMEDIATE q'|CREATE TABLE file_detail
	 ( 
	   file_detail_id	NUMBER		NOT NULL,
	   file_label 		VARCHAR2(50)	NOT NULL,
	   file_group 		VARCHAR2(64)	NOT NULL,
	   label_type 		VARCHAR2(7)	NOT NULL,
	   directory	        VARCHAR2(30),
	   filename	        VARCHAR2(200),
	   source_directory	VARCHAR2(30),
	   source_filename 	VARCHAR2(200),
           archive_filename     VARCHAR2(200),
	   num_bytes 		NUMBER 		NOT NULL,
	   num_lines 		NUMBER,
	   file_dt 		DATE		NOT NULL,
	   label_file		BLOB,
           store_original_files VARCHAR2( 3 ),
           compress_method	VARCHAR2( 20 ),
           encrypt_method	VARCHAR2( 20 ),
           passphrase       	VARCHAR2( 100 ),
	   processed_ts 	TIMESTAMP 	DEFAULT systimestamp NOT NULL,
	   session_id		NUMBER 		DEFAULT sys_context('USERENV','SESSIONID') NOT NULL
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_detail ADD 
	 (
	   CONSTRAINT file_detail_pk
	   PRIMARY KEY
	   (file_detail_id)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_detail ADD 
	 (
	   CONSTRAINT file_detail_fk1
	   FOREIGN KEY ( file_label )
	   REFERENCES file_conf
	   ( file_label )
	 )|';

         EXECUTE IMMEDIATE q'|CREATE SEQUENCE file_detail_seq|';

         -- FILE_OBJECT_DETAIL table
         EXECUTE IMMEDIATE q'|CREATE TABLE file_object_detail
	 ( 
	   file_object_detail_id    NUMBER NOT NULL,
	   file_label 	         VARCHAR2(30) NOT NULL,
	   file_group 	      	 VARCHAR2(50) NOT NULL,
	   label_type 	      	 VARCHAR2(7) NOT NULL,
	   object_owner  	 VARCHAR2(30) NOT NULL,
	   object_name  	 VARCHAR2(30) NOT NULL,
	   processed_ts 	 TIMESTAMP DEFAULT systimestamp NOT NULL,
	   num_rows 	      	 NUMBER,
	   num_lines 	      	 NUMBER,
	   percent_diff 	 NUMBER,
	   session_id 	      	 NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT NULL
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE file_object_detail ADD 
	 (
	   CONSTRAINT file_object_detail_pk
	   PRIMARY KEY
	   (file_object_detail_id)
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|CREATE SEQUENCE file_object_detail_seq|';

         -- TD_PART_GTT table
         EXECUTE IMMEDIATE q'|CREATE global TEMPORARY TABLE td_part_gtt
	 ( 
	   table_owner        VARCHAR2(30),
	   table_name         VARCHAR2(30),
	   partition_name     VARCHAR2(30),
	   partition_position NUMBER,
           partid             VARCHAR2(30)
	 )
	 ON COMMIT DELETE ROWS|';
	 
         EXECUTE IMMEDIATE q'|CREATE global temporary table ddl_queue
	 ( 
	   stmt_ddl 	    VARCHAR2(4000) NOT NULL,
	   stmt_msg 	    VARCHAR2(4000) NOT NULL,
	   client_info	    VARCHAR2(64),
	   module 	    VARCHAR2(48),
	   action 	    VARCHAR2(24),
	   stmt_order 	    NUMBER
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
	   mapping_name		      VARCHAR2(40),
	   mapping_type		      VARCHAR2(10),
	   table_owner 		      VARCHAR2(61),
	   table_name 		      VARCHAR2(30),
	   partition_name             VARCHAR2(30),
	   manage_indexes 	      VARCHAR2(7) NOT NULL,
	   index_concurrency 	      VARCHAR2(3) NOT NULL,
	   manage_constraints 	      VARCHAR2(7) NOT NULL,
	   constraint_concurrency     VARCHAR2(3) NOT NULL,
	   source_owner 	      VARCHAR2(30),
	   source_object 	      VARCHAR2(30),
	   source_column 	      VARCHAR2(30),
	   replace_method 	      VARCHAR2(10),
	   statistics 		      VARCHAR2(10),
	   index_regexp 	      VARCHAR2(30),
	   index_type 		      VARCHAR2(30),
	   partition_type	      VARCHAR2(30),
	   constraint_regexp 	      VARCHAR2(100),
	   constraint_type 	      VARCHAR2(100),
	   description		      VARCHAR2(2000),
	   created_user	     	      VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     	      DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  	      VARCHAR2(30),
	   modified_dt    	      DATE
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD 
	 (
	   CONSTRAINT mapping_conf_pk
	   PRIMARY KEY
	   ( mapping_name )
	   USING INDEX
	 )|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck1 CHECK (mapping_name=lower(mapping_name))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck2 CHECK (manage_indexes in ('usable','unusable','both','ignore'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck3 CHECK (manage_constraints in ('enable','disable','both','ignore'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck4 CHECK (index_concurrency in ('yes','no'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck5 CHECK (constraint_concurrency in ('yes','no'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck6 CHECK (replace_method in ('exchange','rename'))|';

         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck7 CHECK (replace_method = case when table_owner <> source_owner and mapping_type = 'table' then 'exchange' else replace_method end )|';
	 
         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck8 CHECK (mapping_type in ('dimension','table'))|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck9 CHECK (partition_type in ('local','global','all'))|';
         
         EXECUTE IMMEDIATE q'|ALTER TABLE mapping_conf ADD CONSTRAINT mapping_conf_ck10 CHECK (statistics in ('gather','transfer','ignore'))|';

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
	 grant_transcend_rep_privs( p_grantee=> p_schema||'_sel', p_mode => 'select');

	 -- grant all privileges to the admin role
	 grant_transcend_rep_privs( p_grantee=> p_schema||'_adm', p_mode => 'admin');

      EXCEPTION
         WHEN e_tab_exists OR e_stat_tab_exists
         THEN
            RAISE repo_obj_exists;
      END;

      -- the reason for this update is to set the product from 'evolve' to 'transcend'      
      UPDATE tdsys.repositories
	 SET modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	     modified_dt = SYSDATE,
	     product = transcend_product,
	     version = product_version
       WHERE repository_name=upper( p_schema );
      
      IF SQL%ROWCOUNT = 0
      THEN
	 RAISE no_sys_repo_entry;
      END IF;

   EXCEPTION
      WHEN OTHERS
      THEN
         -- if the default tablespace was changed, then put it back
         reset_default_tablespace;
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END build_transcend_repo;

   PROCEDURE build_repository(
      p_schema       VARCHAR2,
      p_tablespace   VARCHAR2,
      p_product      VARCHAR2 DEFAULT TRANSCEND_PRODUCT,
      p_drop         BOOLEAN  DEFAULT FALSE
   )
   IS
   BEGIN
      
      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      create_user( p_user => p_schema, p_tablespace => p_tablespace );
      -- alter session to CURRENT_SCHEMA
      set_current_schema( p_schema => p_schema );

      -- always need to build the evolve repository
      -- this is certainly the case now
      -- I assume all TD products will always be build on Evolve
      build_evolve_repo( p_schema      => p_schema,
                         p_tablespace  => p_tablespace,
                         p_drop        => p_drop );
      
      -- now call any other products required
      -- currently, this is only Transcend if Transcend is specified
      IF p_product = TRANSCEND_PRODUCT
      THEN
         build_transcend_repo( p_schema      => p_schema,
                               p_drop        => p_drop );
      END IF;
      
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      -- set current_schema back to where it started
      reset_current_schema;

   END build_repository;

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
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.RESULTS_TABLE for ' || p_schema || '.RESULTS_TABLE';
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
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.COMMAND_CONF for ' || p_schema || '.COMMAND_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.MODULE_CONF for ' || p_schema
                           || '.MODULE_CONF';
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
                           || '.NOTIFICATION_EVENT for '
                           || p_schema
                           || '.NOTIFICATION_EVENT';
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
            
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG for ' || p_schema || '.LOG';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_runtime for ' || p_schema || '.LOG_runtime';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;
            
      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_runtime_today for ' || p_schema || '.LOG_runtime_today';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_today for ' || p_schema || '.LOG_today';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_week for ' || p_schema || '.LOG_week';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_my_session for ' || p_schema || '.LOG_my_session';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_runtime_my_session for ' || p_schema || '.LOG_runtime_my_session';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_debug for ' || p_schema || '.LOG_debug';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_debug_today for ' || p_schema || '.LOG_debug_today';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.LOG_debug_my_session for ' || p_schema || '.LOG_debug_my_session';
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
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.TD_CORE for ' || p_schema || '.TD_CORE';
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
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.FILE_CONF for ' || p_schema || '.FILE_CONF';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.FILE_DETAIL for ' || p_schema
                           || '.FILE_DETAIL';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILE_DETAIL_SEQ for '
                           || p_schema
                           || '.FILE_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILE_OBJECT_DETAIL for '
                           || p_schema
                           || '.FILE_OBJECT_DETAIL';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILE_OBJECT_DETAIL_SEQ for '
                           || p_schema
                           || '.FILE_OBJECT_DETAIL_SEQ';
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
         EXECUTE IMMEDIATE 'create or replace synonym ' || p_user || '.DDL_QUEUE for ' || p_schema || '.DDL_QUEUE';
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
                           || '.FILE_DETAIL_SEQ for '
                           || p_schema
                           || '.FILE_DETAIL_SEQ';
      EXCEPTION
         WHEN e_same_name
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE    'create or replace synonym '
                           || p_user
                           || '.FILE_OBJECT_DETAIL_SEQ for '
                           || p_schema
                           || '.FILE_OBJECT_DETAIL_SEQ';
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

   PROCEDURE grant_trans_files_sys_privs( p_grantee VARCHAR2 DEFAULT trans_files_role )
   IS
   BEGIN

      -- grant full java permissions needed to manipulate any file and perform any action at the OS level to the TRANS_FILES_SYS role
      DBMS_JAVA.set_output( 1000000 );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'execute' );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'read' );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'write' );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.io.FilePermission', '<<ALL FILES>>', 'delete' );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', '' );
      DBMS_JAVA.grant_permission( UPPER( p_grantee ), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor', '' );

   EXCEPTION
      WHEN OTHERS
      THEN
         -- set current_schema back to where it started
         reset_current_schema;
         RAISE;
   END grant_trans_files_sys_privs;

   PROCEDURE grant_trans_etl_sys_privs( p_grantee VARCHAR2 DEFAULT trans_etl_role )
   IS
   BEGIN
      BEGIN
         -- for each system privilege, grant it to the application owner and the _SYS role
         EXECUTE IMMEDIATE 'GRANT ALTER ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT INSERT ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT SELECT ANY dictionary TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT SELECT ANY SEQUENCE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT UPDATE ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT DELETE ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT ALTER ANY INDEX TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT CREATE ANY INDEX TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT DROP ANY INDEX TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT DROP ANY TABLE TO ' || p_grantee;

         EXECUTE IMMEDIATE 'GRANT ANALYZE ANY TO ' || p_grantee;
      EXCEPTION
         WHEN e_no_obj
         THEN
            raise_application_error( -20004, 'Some repository objects do not exist.' );
      END;
   END grant_trans_etl_sys_privs;

   PROCEDURE build_evolve_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- grant required permissions for code compilation
      BEGIN
	 EXECUTE IMMEDIATE 'GRANT RESOURCE TO ' || p_schema;
      EXCEPTION
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant the RESOURCE role. RESOURCE needs to be granted to user '||p_schema||'.' );
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'GRANT ALTER SESSION TO ' || p_schema;
      EXCEPTION
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant the ALTER SESSION system privilege. ALTER SESSION needs to be granted to user '||p_schema||'.' );
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO ' || p_schema;
      EXCEPTION
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant the SELECT ANY DICTIONARY system privilege. RESOURCE needs to be granted to user '||p_schema||'.' );
      END;

      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.utl_mail TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	    dbms_output.put_line( 'The installing user cannot see package UTL_MAIL. The package needs to be created, and EXECUTE needs to be granted to user '||p_schema||'.' );
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant execute on UTL_MAIL. EXECUTE needs to be granted to user '||p_schema||'.' );
      END;

      -- grant permissions on DBMS_LOCK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_lock TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	    dbms_output.put_line( 'The installing user cannot see package DBMS_LOCK. The package needs to be created, and EXECUTE needs to be granted to user '||p_schema||'.' );
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant execute on DBMS_LOCK. EXECUTE needs to be granted to user '||p_schema||'.' );
      END;

      -- grant permissions on DBMS_FLASHBACK
      -- if the package doesn't exist, or the user doesn't have access to see it, then fail
      BEGIN
         EXECUTE IMMEDIATE 'GRANT EXECUTE ON sys.dbms_flashback TO ' || p_schema;
      EXCEPTION
         WHEN e_no_obj OR e_no_tab
         THEN
	    dbms_output.put_line( 'The installing user cannot see package DBMS_FLASHBACK. The package needs to be created, and EXECUTE needs to be granted to user '||p_schema||'.' );
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The installing user cannot grant execute on DBMS_FLASHBACK. EXECUTE needs to be granted to user '||p_schema||'.' );
      END;
            
      -- grant needed permissions, but not needed for code compilation
      -- Java permissions do not affect PL/SQL code compilation
      dbms_java.grant_permission( upper(p_schema), 'SYS:java.lang.RuntimePermission', 'writeFileDescriptor', NULL );
      dbms_java.grant_permission( upper(p_schema), 'SYS:java.lang.RuntimePermission', 'readFileDescriptor', NULL );
      
      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );

      -- drop all the code objects if they exist
      drop_evolve_app( p_schema => p_schema );
      
      -- create grants to the application owner to all the tables in the repository
      grant_evolve_rep_privs( p_grantee => p_schema );

      -- set the CURRENT_SCHEMA back
      reset_current_schema;

      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );
      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user => p_schema, p_schema => p_repository );
      -- create the dbms_scheduler program
      create_scheduler_metadata;

   END build_evolve_app;

   PROCEDURE build_transcend_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- set CURRENT_SCHEMA to the owner of the repository
      set_current_schema( p_schema => p_repository );

      -- drop all the transcend application objects in order to make sure they can be recreated
      drop_transcend_app( p_schema => p_schema );

      -- create grants to the application owner to all the tables in the repository
      grant_transcend_rep_privs( p_grantee => p_schema, p_mode => 'admin' );

      -- set the CURRENT_SCHEMA back
      reset_current_schema;

      -- set the CURRENT_SCHEMA to the application owner
      set_current_schema( p_schema => p_schema );

      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user => p_schema, p_schema => p_repository );

      -- drop the _SYS roles      
      BEGIN
         EXECUTE IMMEDIATE 'DROP ROLE '||trans_etl_role;
      EXCEPTION
         when e_no_role
         THEN
         NULL;
      END;
      
      BEGIN
         EXECUTE IMMEDIATE 'DROP ROLE '||trans_files_role;
      EXCEPTION
         when e_no_role
         THEN
         NULL;
      END;
      
      -- create _SYS roles
      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || trans_etl_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;

      BEGIN
         EXECUTE IMMEDIATE 'CREATE ROLE ' || trans_files_role;
      EXCEPTION
         WHEN e_role_exists
         THEN
            NULL;
      END;
      
      -- grant full system privileges for Transcend ETL (trans_etl) to the trans_etl role
      grant_trans_etl_sys_privs( p_grantee => trans_etl_role );
      -- grant full system privileges for Transcend Files (trans_files) to the trans_files role
      grant_trans_files_sys_privs( p_grantee => trans_files_role );
      
   END build_transcend_app;

   PROCEDURE build_application(
      p_schema       VARCHAR2,
      p_repository   VARCHAR2,
      p_product      VARCHAR2 DEFAULT TRANSCEND_PRODUCT
   )
   IS
   BEGIN
      
      -- create the user if it doesn't already exist
      create_user( p_user => p_schema );
      
      -- reset the current schema
      reset_current_schema;
      
      -- always install the evolve piece
      build_evolve_app( p_schema      => p_schema,
                        p_repository  => p_repository );


      -- now build the transcend piece if necessary
      IF lower( p_product ) = transcend_product
      THEN
         build_transcend_app( p_schema      => p_schema,
                              p_repository  => p_repository );

      END IF; 


      -- write application tracking record
      UPDATE tdsys.applications
	 SET repository_name = upper( p_repository ),
	     product = lower( p_product ),
	     version = product_version,
	     modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	     modified_dt = SYSDATE
       WHERE application_name=upper( p_schema );
      
      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO tdsys.applications
		( application_name,
		  repository_name,
		  product,
		  version )
		VALUES
		( upper( p_schema ),
		  upper( p_repository ),
		  lower( p_product ),
		  product_version );
      END IF;

      DBMS_OUTPUT.put_line(    ' The CURRENT_SCHEMA is set to '
                            || SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' )
                            || ' in preparation for installing application'
                          );
      

   END build_application;

   PROCEDURE register_evolve_user(
      p_user          VARCHAR2,
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository    VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
      l_adm_role VARCHAR2(30) := p_repository || '_adm';
   BEGIN
      -- create the synonyms to the repository
      build_evolve_rep_syns( p_user => p_user, p_schema => p_repository );
      -- create the synonyms to the application
      build_evolve_app_syns( p_user => p_user, p_schema => p_application );

      -- grant execute on the framework to the new user
      grant_evolve_app_privs( p_user=> p_user, p_schema => p_application );
      
      -- grant permissions to use the Oracle Scheduler
      -- need the permission to create a job
      -- this is for concurrent processing
      BEGIN
         EXECUTE IMMEDIATE 'GRANT create job TO ' || p_user;
      EXCEPTION
	 WHEN e_ins_privs
	 THEN
	    dbms_output.put_line( 'The executing user cannot grant the CREATE JOB system privilege. CREATE JOB needs to be granted to user '||p_user||'.' );
      END;
      
      BEGIN
	 EXECUTE IMMEDIATE 'grant ' || l_adm_role || ' to ' || p_user;
      EXCEPTION
	 WHEN e_ins_privs
	 THEN
	 dbms_output.put_line( 'The executing user cannot grant the role ' || l_adm_role || '. '||l_adm_role||' needs to be granted to user '||p_user||'.' );
      END;

      -- write audit record for creating or modifying a user record
      UPDATE tdsys.users
	 SET application_name = upper( p_application ),
	     repository_name = upper( p_repository ),
	     version = product_version,
	     product = evolve_product,
	     modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	     modified_dt = SYSDATE
       WHERE user_name=upper( p_user );

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO tdsys.users
		( user_name,
		  application_name,
		  repository_name,
		  product,
		  version )
		VALUES
		( upper( p_user ),
		  upper( p_application ),
		  upper( p_repository ),
		  evolve_product,
		  product_version );
      END IF;
   END register_evolve_user;

   PROCEDURE register_transcend_user(
      p_user          VARCHAR2,
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository    VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
   BEGIN
      -- register as an evolve user first
      register_evolve_user( p_user => p_user, p_application => p_application, p_repository => p_repository );

      -- create the synonyms to the repository
      build_transcend_rep_syns( p_user => p_user, p_schema => p_repository );
      -- create the synonyms to the application
      build_transcend_app_syns( p_user => p_user, p_schema => p_application );
      
      -- grant execute on the framework to the new user
      grant_transcend_app_privs( p_user=> p_user, p_schema => p_application );
      
      -- write audit record for modifying a user record
      UPDATE tdsys.users
	 SET application_name = upper( p_application ),
	     repository_name = upper( p_repository ),
	     version = product_version,
	     product = transcend_product,
	     modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
	     modified_dt = SYSDATE
       WHERE user_name=upper( p_user );

   END register_transcend_user;

   PROCEDURE register_user(
      p_user          VARCHAR2,
      p_application   VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository    VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   )
   IS
      l_product   tdsys.applications.product%type;
   BEGIN

      -- find out which product the user is registered for
      l_product := get_product_name( p_application );
      
      -- now call the appropriate procedure
      CASE l_product
         WHEN transcend_product
         THEN 
            register_transcend_user( p_user => p_user,
                                     p_application => p_application,
                                     p_repository => p_repository );
         WHEN evolve_product
         THEN 
            register_evolve_user( p_user => p_user,
                                  p_application => p_application,
                                  p_repository => p_repository );
      END CASE;

   END register_user;
   
   PROCEDURE register_directory (
      p_directory           VARCHAR2,
      p_application         VARCHAR2,
      p_user                VARCHAR2 DEFAULT NULL
   )
   IS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      -- get the directory path, as Java deals with paths, not directory objects
      BEGIN

         SELECT directory_path
           INTO l_path
           FROM all_directories
          WHERE directory_name = UPPER( p_directory );
      EXCEPTION
         WHEN no_data_found
            THEN
            raise_application_error( -20010, 'The specified directory object does not exist' );
      END;
            
      -- now grant the permissions to the application owner
      dbms_java.grant_permission( upper( p_application ), 'SYS:java.io.FilePermission', l_path, 'read' );
      dbms_java.grant_permission( upper( p_application ), 'SYS:java.io.FilePermission', l_path || '/-', 'read' );
      dbms_java.grant_permission( upper( p_application ), 'SYS:java.io.FilePermission', l_path || '/*', 'write,delete' );
      
      -- if a user was specified, then give the permissions there as well
      IF p_user IS NOT NULL
      THEN
         dbms_java.grant_permission( upper( p_user ), 'SYS:java.io.FilePermission', l_path, 'read' );
         dbms_java.grant_permission( upper( p_user ), 'SYS:java.io.FilePermission', l_path || '/-', 'read' );
         dbms_java.grant_permission( upper( p_user ), 'SYS:java.io.FilePermission', l_path || '/*', 'write,delete' );
      END IF;
      
   END register_directory;

   PROCEDURE grant_execute_command (
      p_application     VARCHAR2,
      p_user            VARCHAR2   DEFAULT NULL,
      p_name            VARCHAR2   DEFAULT NULL 
   )
   IS
      TYPE com_conf_tt IS TABLE OF VARCHAR2(200);
      t_com com_conf_tt;
      l_sql VARCHAR2(2000);
   BEGIN

      -- bulk collect the list of commands into a type
      -- if p_command is null, then grant on all commands      
      EXECUTE immediate
      q'|SELECT regexp_replace( path || CASE WHEN path IS NULL THEN NULL ELSE '/' end || value, '//','/' ) command
           FROM |'||p_application||q'|.command_conf
          WHERE REGEXP_LIKE( name, NVL( :b_name, '.' ), 'i' )|'
      bulk collect INTO t_com
      USING p_name;
      
      for i IN 1 .. t_com.count
      LOOP
         
         -- now grant the permissions to the application owner
         dbms_java.grant_permission( upper( p_application ), 'SYS:java.io.FilePermission', t_com(i), 'execute' );
         
         -- if a user was specified, then give the permissions there as well
         IF p_user IS NOT NULL
         THEN
            dbms_java.grant_permission( upper( p_user ), 'SYS:java.io.FilePermission', t_com(i), 'execute' );
         END IF;

      END LOOP;

         -- also grant to the user if specified
   END grant_execute_command;
   
   PROCEDURE backup_tables(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      
      FOR c_tab IN ( SELECT 'create table '
                            ||owner
                            ||'.'
                            ||table_name
                            ||'_'
                            ||product_version
                            ||CASE 
                                 WHEN p_tablespace IS NULL 
                                 THEN NULL 
                                 ELSE ' tablespace '||p_tablespace 
                              END 
                            ||' as select * from '||table_name DDL
                       FROM all_tables
                      WHERE owner=upper( p_schema )
                   )
      LOOP
         
         EXECUTE IMMEDIATE c_tab.ddl;
         
      END LOOP;
         

   END backup_tables;
   
   PROCEDURE drop_tables(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      
      FOR c_tab IN ( SELECT 'drop table '
                            ||owner
                            ||'.'
                            ||table_name DDL
                       FROM all_tables
                      WHERE owner=upper( p_schema )
                   )
      LOOP
         
         EXECUTE IMMEDIATE c_tab.ddl;
         
      END LOOP;

   END drop_tables;

END td_adm;
/

SHOW errors