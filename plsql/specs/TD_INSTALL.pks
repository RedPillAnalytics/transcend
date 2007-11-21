CREATE OR REPLACE PACKAGE td_evolve_install AUTHID CURRENT_USER
IS
   PROCEDURE create_rep_user(
      p_user        VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   );
      
   PROCEDURE create_stats_table(
      p_owner  VARCHAR2 DEFAULT 'TDSYS',
      p_table  VARCHAR2 DEFAULT 'OPT_STATS'
   );

   PROCEDURE reset_default_tablespace(
      p_owner  VARCHAR2 DEFAULT 'TDSYS'
   );

END td_evolve_install;
/