CREATE OR REPLACE PACKAGE td_adm AUTHID CURRENT_USER
IS

   td_version CONSTANT NUMBER := 1.3;

   default_repository CONSTANT VARCHAR2(6) := 'TDREP';

   repo_obj_exists EXCEPTION;
   no_sys_repo_entry EXCEPTION;

   PROCEDURE build_sys_repo(
      p_schema       VARCHAR2 DEFAULT 'TDSYS',
      p_tablespace   VARCHAR2 DEFAULT 'TDSYS'
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

   PROCEDURE grant_evolve_app_privs(
      p_user     VARCHAR2,
      p_schema   VARCHAR2 DEFAULT 'TDSYS'    
   );

   PROCEDURE grant_transcend_app_privs(
      p_user     VARCHAR2,
      p_schema   VARCHAR2 DEFAULT 'TDSYS'    
   );

   PROCEDURE drop_evolve_repo(
      p_schema   VARCHAR2 DEFAULT 'TDSYS'
   );

   PROCEDURE drop_evolve_app;

   PROCEDURE drop_transcend_types;

   PROCEDURE create_evolve_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT 'TDSYS', 
      p_repository   VARCHAR2 DEFAULT 'TDSYS'
   );
   
   PROCEDURE create_transcend_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT 'TDSYS', 
      p_repository   VARCHAR2 DEFAULT 'TDSYS'
   );
      
END td_adm;
/