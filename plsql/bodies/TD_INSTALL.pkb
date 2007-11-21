CREATE OR REPLACE PACKAGE BODY td_evolve_install
IS
   g_tablespace all_users.tablespace_name;

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

	 -- get the current default tablespace of the repository user
	   SELECT default_tablespace
	     INTO g_old_tbspace
	     FROM all_users
	    WHERE username=upper(p_user);
	 
	   EXECUTE IMMEDIATE 'alter user '||p_user||' default tablespace '||p_tablespace;

	 WHEN e_no_tbspace
	 THEN
	   raise_application_error(-20001,'Tablespace '||p_tbspace||' does not exist');
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
	 DBMS_STATS.CREATE_STAT_TABLE('&rep_schema_tr','OPT_STATS')
      EXCEPTION
	 WHEN e_tab_exists
	 THEN
	   dbms_output.put_line('Statistics table already exists');
      END;      
   END create_stats_table;

   PROCEDURE reset_default_tablespace(
      p_owner  VARCHAR2 DEFAULT 'TDSYS'
   ) 
   IS
   BEGIN
      IF g_tablespace IS NOT null
      THEN
	 EXECUTE IMMEDIATE 'alter user '||p_owner||' default tablespace '||g_tablespace;
      END IF;
   END reset_default_tablespace;
   
END td_evolve_install;
/