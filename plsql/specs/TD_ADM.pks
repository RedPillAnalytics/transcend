CREATE OR REPLACE PACKAGE td_adm AUTHID CURRENT_USER
IS

   version CONSTANT NUMBER := 1.3;

   evolve_sys_role CONSTANT VARCHAR2(30) := 'EVOLVE_SYS';
   trans_etl_role CONSTANT VARCHAR2(30) := 'TRANS_ETL_SYS';
   trans_files_role CONSTANT VARCHAR2(30) := 'TRANS_FILES_SYS';

   default_repository CONSTANT VARCHAR2(6) := 'TDREP';

   repo_obj_exists EXCEPTION;
   no_sys_repo_entry EXCEPTION;

   PROCEDURE build_evolve_repo(
      p_schema      VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace  VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop	    BOOLEAN  DEFAULT FALSE
   );
      
   PROCEDURE build_transcend_repo(
      p_schema      VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace  VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_drop	    BOOLEAN  DEFAULT FALSE
   );
      
   PROCEDURE build_evolve_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );
      
   PROCEDURE build_transcend_app(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );

   PROCEDURE grant_evolve_app_privs(
      p_user     VARCHAR2,
      p_schema   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );

   PROCEDURE grant_transcend_app_privs(
      p_user     VARCHAR2,
      p_schema   VARCHAR2 DEFAULT DEFAULT_REPOSITORY    
   );

   PROCEDURE drop_evolve_repo(
      p_schema   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );

   PROCEDURE drop_evolve_app;

   PROCEDURE drop_transcend_types;

   PROCEDURE create_evolve_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT DEFAULT_REPOSITORY, 
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );
   
   PROCEDURE create_transcend_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT DEFAULT_REPOSITORY, 
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );
      
END td_adm;
/