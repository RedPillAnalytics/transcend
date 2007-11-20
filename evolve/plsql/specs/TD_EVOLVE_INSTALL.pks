CREATE OR REPLACE PACKAGE td_evolve_install AUTHID CURRENT_USER
IS
   PROCEDURE create_rep_user(
      p_rep_user        VARCHAR2 DEFAULT 'TDSYS',
      p_rep_tablespace  VARCHAR2 DEFAULT 'TDSYS'
   );

END td_evolve_install;
/