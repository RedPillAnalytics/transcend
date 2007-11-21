CREATE OR REPLACE PACKAGE BODY td_evolve_adm
IS
   g_old_tbspace all_users.tablespace_name;

   PROCEDURE create_rep_user(
      p_rep_user     VARCHAR2 DEFAULT 'TDSYS',
      p_rep_tbspace  VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
      e_user_exists EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
      e_no_tbspace	 EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
   BEGIN
      BEGIN
	 EXECUTE IMMEDIATE 'CREATE USER '||p_rep_user||' identified by no2'||p_rep_user||' default tablespace '||p_rep_tbspace;
      EXCEPTION
	 WHEN e_user_exists
	 THEN

	 -- get the current default tablespace of the repository user
	   SELECT default_tablespace
	     INTO g_old_tbspace
	     FROM all_users
	    WHERE username=upper('&rep_schema_cru');
	 
	   EXECUTE IMMEDIATE 'alter user '||p_rep_user||' default tablespace '||p_tbspace;

	 WHEN e_no_tbspace
	 THEN
	   raise_application_error(-20001,'Tablespace '||p_tbspace||' does not exist');
      END;

   END check_module;


END td_evolve_adm;
/