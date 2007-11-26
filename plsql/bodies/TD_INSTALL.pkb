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

   PROCEDURE create_stats_table(
      p_owner  VARCHAR2 DEFAULT 'TDSYS',
      p_table  VARCHAR2 DEFAULT 'OPT_STATS'
   ) 
   IS
      e_tab_exists EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_tab_exists, -20002 );
   BEGIN
      BEGIN
	 DBMS_STATS.CREATE_STAT_TABLE( p_owner, p_table );
      EXCEPTION
	 WHEN e_tab_exists
	 THEN
	   dbms_output.put_line('Statistics table already exists');
      END;      
   END create_stats_table;

   PROCEDURE reset_default_tablespace
   IS
   BEGIN
      IF g_tablespace IS NOT NULL AND g_user IS NOT null
      THEN
	 EXECUTE IMMEDIATE 'alter user '||g_user||' default tablespace '||g_tablespace;
      END IF;
   END reset_default_tablespace;

   PROCEDURE build_sys_repo(
      p_owner VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
   BEGIN
      
      create_user( p_owner, p_tablespace );
      
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

      reset_default_tablespace;

   END build_sys_repo;


   PROCEDURE build_repo(
      p_owner VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
   BEGIN
      
      -- create the repository owner
      create_user( p_owner, p_tablespace );
      
      -- create the statitics table
      create_stats_table( p_owner, p_tablespace );

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
      
      reset_default_tablespace;

   END build_repo;
   
END td_install;
/