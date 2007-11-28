CREATE OR REPLACE PACKAGE td_install AUTHID CURRENT_USER
IS
   PROCEDURE grant_evolve_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS',
      p_drop     BOOLEAN  DEFAULT FALSE    
   );
      
   PROCEDURE grant_transcend_privs(
      p_schema   VARCHAR2 DEFAULT 'TDSYS'  
   );

   PROCEDURE build_sys_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   );
      
   PROCEDURE build_evolve_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   );
      
   PROCEDURE build_transcend_repo(
      p_schema      VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace  VARCHAR2 DEFAULT 'TDSYS',
      p_drop	    BOOLEAN  DEFAULT FALSE
   );

END td_install;
/