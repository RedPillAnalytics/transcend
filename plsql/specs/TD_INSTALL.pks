CREATE OR REPLACE PACKAGE td_install AUTHID CURRENT_USER
IS
   PROCEDURE create_user(
      p_user        VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   );
      
   PROCEDURE create_stats_table(
      p_owner  VARCHAR2 DEFAULT 'TDSYS',
      p_table  VARCHAR2 DEFAULT 'OPT_STATS'
   );

   PROCEDURE reset_default_tablespace;

   PROCEDURE build_sys_repo(
      p_owner VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   );
      
   PROCEDURE build_repo(
      p_owner VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   );

END td_install;
/