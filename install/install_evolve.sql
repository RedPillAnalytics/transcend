SET echo off
SET verify off
SET serveroutput on size unlimited
SET timing off

DEFINE product = 'evolve'

ALTER SESSION SET nls_date_format = 'yyyymmdd_hhmiss';
DEFINE suffix = _&_DATE..log
SPOOL install_&product&suffix

-- get the schema for the Evolve application (PL/SQL and Java code)
ACCEPT app_schema char default 'TDREP' prompt 'Schema name for the application [tdrep]: '
-- get the schema for the Evolve repository (tables)
ACCEPT rep_schema char default 'TDREP' prompt 'Schema name for the default repository for this application [tdrep]: '
-- get the tablespace for the repository
ACCEPT tablespace char default 'TDREP' prompt 'Tablespace in which to install default repository: [tdrep]: '

WHENEVER sqlerror exit sql.sqlcode

------------------------------
-- CREATE THE TDSYS REPOSITORY
------------------------------

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

-- go ahead and create the package spec for TD_ADM
-- this makes the constants in the spec available
@../plsql/specs/TD_ADM.pks

VARIABLE b_role_exists char(1)
SET feedback off
DECLARE
   e_role_exists EXCEPTION;
   PRAGMA exception_init( e_role_exists, -1921 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE q'|CREATE ROLE evolve|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_etl|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_files|';
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
   IF :b_role_exists = 'Y' AND upper('&drop_repo') = 'Y'
   THEN
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE evolve|';
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE trans_etl|';
      EXCEPTION
	 WHEN e_no_role
	 THEN
	 NULL;
      END;
      BEGIN
	 EXECUTE IMMEDIATE q'|DROP ROLE trans_files|';
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
      EXECUTE IMMEDIATE q'|CREATE ROLE evolve|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_etl|';
      EXECUTE IMMEDIATE q'|CREATE ROLE trans_files|';

   ELSIF :b_role_exists = 'Y' AND '&drop_repo' = 'N'
   THEN
      raise_application_error(-20004, 'Installation aborted by user.' );
   ELSE
      NULL;
   END IF;
END;
/

SET feedback on

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

-- now create the package body
@../plsql/wrapped_bodies/TD_ADM.plb

BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema='||:b_current_schema;
END;
/


-------------------------------------------
-- INSTALL PRODUCT (Either Evolve or Transcend)
-------------------------------------------

DECLARE
   l_drop BOOLEAN := CASE WHEN REGEXP_LIKE('yes','&drop_repo','i') THEN TRUE ELSE FALSE END;
BEGIN
   -- create the Evolve repository
   tdsys.td_adm.build_repository( p_schema => '&rep_schema', p_product => lower('&product'), p_tablespace => '&tablespace', p_drop => l_drop );
   -- create the Evolve application
   tdsys.td_adm.build_application( p_schema => '&app_schema', p_product => lower('&product'), p_repository => '&rep_schema' );   
EXCEPTION
   WHEN tdsys.td_adm.repo_obj_exists
   THEN
   raise_application_error(-20003,'Repository tables exist. Specify ''Y'' when prompted to issue DROP TABLE statements');
END;
/

-- grant permissions on the tdsys repository
GRANT SELECT ON tdsys.applications TO &app_schema;
GRANT SELECT ON tdsys.repositories TO &app_schema;
GRANT SELECT ON tdsys.users TO &app_schema;

-- grant permissions on the tdsys package
GRANT EXECUTE ON tdsys.td_adm TO &app_schema;

-- we always need to install the Evolve objects

-- this type is created first as it's needed for the TD_CORE
@../evolve/plsql/specs/SPLIT_OT.tps

-- create collection of libraries that make no use of the Evolve repository
-- these don't perform any real SQL at all
-- simply a series of reusable functions that don't have any external dependencies
@../evolve/plsql/specs/TD_CORE.pks

-- non-packaged functions because STRAGG cannot be packaged
@../evolve/plsql/specs/STRING_AGG_OT.tps
@../evolve/plsql/wrapped_bodies/STRAGG.plb

-- create java stored procedures
-- this contains OS and file level utilites that aren't available in other API's
@../evolve/java/TdUtils.jvs

-- layer in the utilities that require repository objects
@../evolve/plsql/specs/TD_INST.pks
@../evolve/plsql/specs/EVOLVE.pks
@../evolve/plsql/specs/NOTIFICATION_OT.tps
@../evolve/plsql/specs/EVOLVE_OT.tps

-- create utilities package that uses the main Evolve framework
@../evolve/plsql/specs/TD_UTILS.pks

-- create callable packages
@../evolve/plsql/specs/EVOLVE_ADM.pks

-- now compile all the package bodies
@../evolve/plsql/wrapped_bodies/STRING_AGG_OT.plb
@../evolve/plsql/wrapped_bodies/TD_CORE.plb
@../evolve/plsql/wrapped_bodies/TD_INST.plb
@../evolve/plsql/wrapped_bodies/EVOLVE.plb
@../evolve/plsql/wrapped_bodies/NOTIFICATION_OT.plb
@../evolve/plsql/wrapped_bodies/EVOLVE_OT.plb
@../evolve/plsql/wrapped_bodies/TD_UTILS.plb
@../evolve/plsql/wrapped_bodies/EVOLVE_ADM.plb

-- set the default logging, registration and runmodes
EXEC evolve_adm.set_default_configs;

SPOOL off
