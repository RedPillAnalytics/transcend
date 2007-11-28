CREATE OR REPLACE PACKAGE td_install AUTHID CURRENT_USER
IS
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
      
   PROCEDURE build_evolve_app(
      p_schema       VARCHAR2 DEFAULT 'TDSYS',
      p_repository   VARCHAR2 DEFAULT 'TDSYS',
      p_drop	     BOOLEAN  DEFAULT FALSE
   );
      
   PROCEDURE build_transcend_app(
      p_schema       VARCHAR2 DEFAULT 'TDSYS',
      p_repository   VARCHAR2 DEFAULT 'TDSYS',
      p_drop	     BOOLEAN  DEFAULT FALSE
   );
      
END td_install;
/