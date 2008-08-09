SET echo off
SET verify off
PROMPT 'Running install_tdsys_repo.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTdsys_&_DATE..log

VARIABLE b_tbspace char(30)
VARIABLE b_current_schema char(30)
-- create the tdsys user if it doesn't already exist
DECLARE
   l_user           all_users.username%TYPE;
BEGIN

   -- get the current schema
   SELECT sys_context('USERENV','CURRENT_SCHEMA')
     INTO :b_current_schema
     FROM dual;   
   BEGIN
      -- see if the TDSYS user exists
      -- while we're at it, get his default tablespace
      SELECT username,
	     default_tablespace
	INTO l_user,
	     :b_tbspace
	FROM dba_users
       WHERE username = 'TDSYS';
   EXCEPTION
      -- the user does not exist
      WHEN NO_DATA_FOUND
      THEN
      -- create it
      EXECUTE IMMEDIATE 'CREATE USER tdsys identified by no2tdsys default tablespace &tablespace quota unlimited on &tablespace';
   END;
END;
/

-- needed to interact with users and their tablespaces
GRANT SELECT ANY dictionary TO tdsys;

ALTER SESSION SET current_schema=tdsys;

VARIABLE b_role_exists char(1)
SET feedback off
DECLARE
   e_role_exists EXCEPTION;
   PRAGMA exception_init( e_role_exists, -1921 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE q'|CREATE ROLE evolve_sys|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_etl_sys|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_files_sys|';
      dbms_output.put_line( 'Press RETURN to continue' );

   EXCEPTION
      WHEN e_role_exists
      THEN
      :b_role_exists := 'Y';
      dbms_output.put_line( 'Some repository objects exist. If this is an upgrade to Evolve or Transcend, then the upgrade script should be run instead.'
			    ||chr(10)
			    ||'To continue, any repository objects will have to be dropped and recreated. Do you want to continue? [N]' );
   END;
END;
/

-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT drop_repo char default 'N'
DECLARE
   e_no_role EXCEPTION;
   e_no_tab  EXCEPTION;
   PRAGMA exception_init( e_no_role, -1919 );
   PRAGMA exception_init( e_no_tab, -942 );
BEGIN
--   dbms_output.put_line('The value of bind variable b_role_exists: '||:b_role_exists);
--   dbms_output.put_line('The value of bind sqlplus variable drop_repo: &drop_repo');
   IF :b_role_exists = 'Y' AND '&drop_repo' = 'Y'
   THEN
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE evolve_sys|';
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE trans_etl_sys|';
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE trans_files_sys|';
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP table tdsys.users|';
      EXCEPTION
	 WHEN e_no_tab
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP table tdsys.applications|';
      EXCEPTION
	 WHEN e_no_tab
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP table tdsys.repositories|';
      EXCEPTION
	 WHEN e_no_tab
	 THEN
	 NULL;
      END;
      EXECUTE IMMEDIATE q'|CREATE ROLE evolve_sys|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_etl_sys|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_files_sys|';

   ELSIF :b_role_exists = 'Y' AND '&drop_repo' = 'N'
   THEN
      raise_application_error(-20004, 'TDSYS repository object exist. Installation cannot continue.' );
   ELSE
      NULL;
   END IF;
END;
/

SET feedback on

-- get the CURRENT_SCHEMA

CREATE TABLE tdsys.repositories
       ( 
	 repository_name    VARCHAR2(30) NOT NULL,
	 product	    VARCHAR2(20),
	 version	    NUMBER,
	 created_user	    VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	    DATE DEFAULT SYSDATE NOT NULL,
	 modified_user      VARCHAR2(30),
	 modified_dt	    DATE
       );

ALTER TABLE tdsys.repositories ADD 
      (
        CONSTRAINT repositories_pk
        PRIMARY KEY
	(repository_name)
	USING INDEX
      );

CREATE TABLE tdsys.applications
       ( 
	 application_name   VARCHAR2(30) NOT NULL,
	 repository_name    VARCHAR2(30) NOT NULL,
	 product	    VARCHAR2(20),
	 version 	    NUMBER,
	 created_user	    VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	    DATE DEFAULT SYSDATE NOT NULL,
	 modified_user      VARCHAR2(30),
	 modified_dt	    DATE
       );

ALTER TABLE tdsys.applications ADD 
      (
	CONSTRAINT applications_pk
	PRIMARY KEY
	(application_name)
	USING INDEX
      );

ALTER TABLE tdsys.applications ADD 
      (
	CONSTRAINT applications_fk1
	FOREIGN KEY (repository_name)
	REFERENCES repositories  
	( repository_name )
      );

CREATE TABLE tdsys.users
       ( 
	 user_name          VARCHAR2(30) NOT NULL,
	 application_name   VARCHAR2(30) NOT NULL,
	 repository_name    VARCHAR2(30) NOT NULL,
	 product	    VARCHAR2(20),
	 version 	    NUMBER,
	 created_user	    VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	    DATE DEFAULT SYSDATE NOT NULL,
	 modified_user      VARCHAR2(30),
	 modified_dt	    DATE
       );

ALTER TABLE tdsys.users ADD 
      (
	CONSTRAINT users_pk
	PRIMARY KEY
	(user_name)
	USING INDEX
      );

ALTER TABLE tdsys.users ADD 
      (
	CONSTRAINT users_fk1
	FOREIGN KEY (repository_name)
	REFERENCES repositories  
	( repository_name )
      );

ALTER TABLE tdsys.users ADD 
      (
	CONSTRAINT users_fk2
	FOREIGN KEY (application_name)
	REFERENCES applications 
	( application_name )
      );

      
-- install the installation package
@../plsql/specs/TD_ADM.pks
@../plsql/wrapped_bodies/TD_ADM.plb

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/

SPOOL off
