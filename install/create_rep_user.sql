PROMPT 'Running create_rep_user.sql'

-- &1 IS the repository schema
DEFINE rep_schema_cru = &1
-- &2 IS the tablespace name
DEFINE tablespace_cru = &2

WHENEVER sqlerror exit sql.sqlcode

VARIABLE old_tbspace char(30)
DECLARE
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
   e_no_tbspace	 EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'CREATE USER &rep_schema_cru identified by no2&rep_schema_cru default tablespace &tablespace_cru';
      EXECUTE IMMEDIATE 'GRANT connect to &rep_schema_cru';
   EXCEPTION
      WHEN e_user_exists
      THEN
      -- get the current default tablespace of the repository user
      SELECT default_tablespace
	INTO :old_tbspace
	FROM dba_users
       WHERE username=upper('&rep_schema_cru');
      EXECUTE IMMEDIATE 'alter user &rep_schema_cru default tablespace &tablespace_cru';
      WHEN e_no_tbspace
      THEN
      raise_application_error(-20001,'Tablespace &tablespace_cru does not exist');
   END;
END;
/
WHENEVER sqlerror continue