CREATE OR REPLACE PACKAGE BODY td_install
IS
   g_user	dba_users.username%TYPE;
   g_tablespace dba_users.default_tablespace%TYPE;

   PROCEDURE create_user(
      p_user        VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      e_user_exists EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
      e_no_tbspace	 EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
   BEGIN
      BEGIN
	 EXECUTE IMMEDIATE 'CREATE USER '||p_user||' identified by no2'||p_user||' default tablespace '||p_tablespace;
      EXCEPTION
	 WHEN e_user_exists
	 THEN
	   g_user := p_user;
	 -- get the current default tablespace of the repository user
	   SELECT default_tablespace
	     INTO g_tablespace
	     FROM dba_users
	    WHERE username=upper(p_user);
	 
	   EXECUTE IMMEDIATE 'alter user '||p_user||' default tablespace '||p_tablespace;

	 WHEN e_no_tbspace
	 THEN
	   raise_application_error(-20001,'Tablespace '||p_tablespace||' does not exist');
      END;
      
      -- gieve the user a quote
      EXECUTE IMMEDIATE 'ALTER USER '||p_user||' QUOTA 50M ON '||p_tablespace;
      
      -- set the session to that user
      EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||p_user;

   END create_user;

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
      EXECUTE IMMEDIATE 'alter session set current_schema=&_USER';
   END reset_current_schema;
   

   PROCEDURE build_sys_repo(
      p_owner       VARCHAR2 DEFAULT 'TDSYS',
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
      -- alter session to become that user      
      create_user( p_owner, p_tablespace );
      
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
	 dbms_output.put_line('Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;
      
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to &_USER
      reset_current_schema;

   END build_sys_repo;


   PROCEDURE build_evolve_repo(
      p_owner       VARCHAR2 DEFAULT 'TDSYS',
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
      -- alter session to become that user      
      create_user( p_owner, p_tablespace );
      
      
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
	   runmode VARCHAR2(10) NOT NULL,
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
	   code NUMBER NOT NULL,
	   name VARCHAR2(30) NOT NULL,
	   message VARCHAR2(1000) NOT NULL,
	   created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	   modified_user	     VARCHAR2(30),
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
      EXCEPTION
	 WHEN e_tab_exists
	 THEN
	 dbms_output.put_line('Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;

      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to &_USER
      reset_current_schema;

   END build_evolve_repo;

   PROCEDURE build_transcend_repo(
      p_owner       VARCHAR2 DEFAULT 'TDSYS',
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
   BEGIN
      
      -- create the user if it doesn't already exist
      -- if it does, then simply change the default tablespace for that user
      -- alter session to become that user      
      create_user( p_owner, p_tablespace );
      
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
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;
	 
	 BEGIN
	    EXECUTE IMMEDIATE q'|DROP sequence files_obj_detail_seq|';
	 EXCEPTION
	    WHEN e_no_tab
	    THEN
	    NULL;
	 END;
	 
      END IF;


      BEGIN
	 -- create the statitics table
	 DBMS_STATS.CREATE_STAT_TABLE( p_owner, 'OPT_STATS' );

	 -- FILES_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE files_conf
	 ( 
	   file_label	       VARCHAR2(100) 	NOT NULL,
	   file_group	       VARCHAR2(64) 	NOT NULL,
	   file_type	       VARCHAR2(7) 	NOT NULL,
	   file_description       VARCHAR2(100),
	   object_owner	       VARCHAR2(30)	NOT NULL,
	   object_name	       VARCHAR2(30)    	NOT NULL,
	   directory	       VARCHAR2(30)	NOT NULL,
	   filename	       VARCHAR2(50)    	NOT NULL,		
	   arch_directory         VARCHAR2(30) 	NOT NULL,
	   min_bytes	       NUMBER 		DEFAULT 0 NOT NULL,
	   max_bytes              NUMBER 		DEFAULT 0 NOT NULL,
	   file_datestamp	       VARCHAR2(30),
	   baseurl                VARCHAR2(500),
	   passphrase             VARCHAR2(100),
	   source_directory       VARCHAR2(50),
	   source_regexp          VARCHAR2(100),
	   regexp_options	       VARCHAR2(10)	DEFAULT 'i',
	   source_policy	       VARCHAR2(10) 	DEFAULT 'newest',
	   required       	       VARCHAR2(1) 	DEFAULT 'Y',
	   delete_source 	       VARCHAR2(3)     	DEFAULT 'Y',
	   reject_limit 	       NUMBER,
	   dateformat	       VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss am',
	   timestampformat	       VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
	   delimiter	       VARCHAR2(1)    	DEFAULT ',',
	   quotechar	       VARCHAR2(2),
	   headers		       VARCHAR2(1),
	   created_user   	       VARCHAR2(30),
	   created_dt     	       DATE,
	   modified_user  	       VARCHAR2(30),
	   modified_dt    	       DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE files_conf ADD 
	 (
	   CONSTRAINT files_conf_pk
	   PRIMARY KEY
	   (file_label, file_group)
	   USING INDEX
	 )|';
	 
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
	   index_owner 	   VARCHAR2(30),
	   index_name 	   VARCHAR2(30),
	   src_index_owner    VARCHAR2(30),
	   src_index_name 	   VARCHAR2(30),
	   create_ddl 	   VARCHAR2(4000),
	   create_msg 	   VARCHAR2(4000),
	   rename_ddl 	   VARCHAR2(4000),
	   rename_msg 	   VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- TD_BUILD_CON_GTT
	 EXECUTE IMMEDIATE
	 q'|CREATE global TEMPORARY TABLE td_build_con_gtt
	 ( 
	   table_owner	      VARCHAR2(30),
	   table_name	      VARCHAR2(30),
	   constraint_name	      VARCHAR2(30),
	   src_constraint_name   VARCHAR2(30),
	   index_name 	      VARCHAR2(30),
	   index_owner 	      VARCHAR2(30),
	   create_ddl 	      VARCHAR2(4000),
	   create_msg 	      VARCHAR2(4000),
	   rename_ddl 	      VARCHAR2(4000),
	   rename_msg 	      VARCHAR2(4000)
	 )
	 ON COMMIT DELETE ROWS|';
	 
	 -- TD_CON_MAINT_GTT
	 EXECUTE IMMEDIATE
	 q'|CREATE global TEMPORARY TABLE td_con_maint_gtt
	 ( 
	   table_owner         VARCHAR2(30),
	   table_name 	    VARCHAR2(30),
	   constraint_name     VARCHAR2(30),
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
	 EXECUTE IMMEDIATE q'|INSERT INTO replace_method_list (replace_method) VALUES ('insert')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO replace_method_list (replace_method) VALUES ('merge')|';
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
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('scd type 3')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('effective start date')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('effective end date')|';
	 EXECUTE IMMEDIATE q'|INSERT INTO column_type_list (column_type) VALUES ('current indicator')|';
	 
	 -- DIMENSION_CONF table
	 EXECUTE IMMEDIATE 
	 q'|CREATE TABLE dimension_conf
	 ( 
	   owner			VARCHAR2(30) NOT NULL,
	   table_name		VARCHAR2(30) NOT NULL,
	   source_owner		VARCHAR2(30) NOT NULL,
	   source_object		VARCHAR2(30) NOT NULL,
	   sequence_owner  	VARCHAR2(30) NOT NULL,
	   sequence_name  	        VARCHAR2(30) NOT NULL,
	   staging_owner		VARCHAR2(30),
	   staging_table		VARCHAR2(30),
	   direct_load		VARCHAR2(3) DEFAULT 'yes' NOT NULL,
	   replace_method		VARCHAR2(10) DEFAULT 'rename' NOT NULL,
	   statistics		VARCHAR2(10),
	   created_user	     	VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	   created_dt	     	DATE DEFAULT SYSDATE NOT NULL,
	   modified_user  		VARCHAR2(30),
	   modified_dt    		DATE
	 )|';
	 
	 EXECUTE IMMEDIATE 
	 q'|ALTER TABLE dimension_conf ADD 
	 (
	   CONSTRAINT dimension_conf_pk
	   PRIMARY KEY
	   ( owner, table_name )
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
	 q'|ALTER TABLE dimension_conf
	 ADD CONSTRAINT replace_method_ck
	 CHECK ( upper(staging_owner) = CASE WHEN replace_method = 'rename' THEN upper(owner) ELSE staging_owner end )|';

	 -- COLUMN_CONF table
	 EXECUTE IMMEDIATE
	 q'|CREATE TABLE column_conf
	 ( 
	   owner		VARCHAR2(30) NOT NULL,
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
	   ( owner, table_name, column_name )
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
	   FOREIGN KEY ( owner, table_name )
	   REFERENCES dimension_conf  
	   ( owner, table_name )
	 )|';
      EXCEPTION
      WHEN e_tab_exists
	 THEN
	   dbms_output.put_line('Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      WHEN e_stat_tab_exists
	 THEN
	   dbms_output.put_line('Repository tables exist. If you want to drop all repository tables, then specifiy a value of TRUE for P_DROP');
      END;
     
      -- if the default tablespace was changed, then put it back
      reset_default_tablespace;
      
      -- set current_schema back to &_USER
      reset_current_schema;

   END build_transcend_repo;
   
END td_install;
/