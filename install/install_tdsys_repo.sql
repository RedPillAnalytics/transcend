SET echo off
SET verify off
PROMPT 'Running install_tdsys_repo.sql'
SET serveroutput on size unlimited
SET timing off
ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
SPOOL InstallTdsys_&_DATE..log

-- create the tdsys user if it doesn't already exist
DECLARE
   l_user           all_users.username%TYPE;
BEGIN
   SELECT username
     INTO l_user
     FROM dba_users
    WHERE username = 'TDSYS';

EXCEPTION
   -- the user does not exist
   WHEN NO_DATA_FOUND
   THEN
      EXECUTE IMMEDIATE 'CREATE USER tdsys identified by no2tdsys default tablespace &tablespace quota unlimited on &tablespace';
END;
/

-- needed to interact with users and their tablespaces
GRANT SELECT ANY dictionary TO tdsys;

ALTER SESSION SET current_schema=tdsys;

-- create the roles for system level privileges
CREATE ROLE evolve_sys;
CREATE ROLE trans_etl_sys;
CREATE ROLE trans_files_sys;
      

-- get the CURRENT_SCHEMA
VARIABLE td_curr_schema char(30)
EXEC :td_curr_schema := sys_context('USERENV','CURRENT_SCHEMA');

-- create all the tables      
CREATE TABLE repositories
       ( 
	 repository_name    VARCHAR2(30) NOT NULL,
	 product	    VARCHAR2(20),
	 version	    NUMBER,
	 created_user	    VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	    DATE DEFAULT SYSDATE NOT NULL,
	 modified_user      VARCHAR2(30),
	 modified_dt	    DATE
       );

ALTER TABLE repositories ADD 
      (
        CONSTRAINT repositories_pk
        PRIMARY KEY
	(repository_name)
	USING INDEX
      );

CREATE TABLE applications
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

ALTER TABLE applications ADD 
      (
	CONSTRAINT applications_pk
	PRIMARY KEY
	(application_name)
	USING INDEX
      );

ALTER TABLE applications ADD 
      (
	CONSTRAINT applications_fk1
	FOREIGN KEY (repository_name)
	REFERENCES repositories  
	( repository_name )
      );

CREATE TABLE users
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

ALTER TABLE users ADD 
      (
	CONSTRAINT users_pk
	PRIMARY KEY
	(user_name)
	USING INDEX
      );

ALTER TABLE users ADD 
      (
	CONSTRAINT users_fk1
	FOREIGN KEY (repository_name)
	REFERENCES repositories  
	( repository_name )
      );

ALTER TABLE users ADD 
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
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:td_curr_schema;
END;
/

SPOOL off
