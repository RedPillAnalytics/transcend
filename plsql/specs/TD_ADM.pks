CREATE OR REPLACE PACKAGE td_adm AUTHID CURRENT_USER
IS
   -- global exceptions
   repo_obj_exists EXCEPTION;
   no_sys_repo_entry EXCEPTION;

   -- package constatnts
   version CONSTANT NUMBER := 2.0; 
   evolve_sys_role CONSTANT VARCHAR2(30) := 'EVOLVE';
   trans_etl_role CONSTANT VARCHAR2(30) := 'TRANS_ETL';
   trans_files_role CONSTANT VARCHAR2(30) := 'TRANS_FILES';
   default_repository CONSTANT VARCHAR2(6) := 'TDREP';


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

   PROCEDURE drop_evolve_app ( p_schema VARCHAR2 );

   PROCEDURE drop_transcend_app ( p_schema VARCHAR2 );

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

   PROCEDURE backup_tables(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT NULL
   );
     
   PROCEDURE drop_tables(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY,
      p_tablespace   VARCHAR2 DEFAULT NULL
   ); 
         
END td_adm;
/