CREATE OR REPLACE PACKAGE td_adm AUTHID CURRENT_USER
IS
   -- global exceptions
   repo_obj_exists      EXCEPTION;
   no_sys_repo_entry    EXCEPTION;
   unknown_user         EXCEPTION;

   -- package constants
   product_version      CONSTANT        NUMBER          := 2.643;
   transcend_product    CONSTANT        VARCHAR2(9)     := 'transcend';
   evolve_product       CONSTANT        VARCHAR2(9)     := 'evolve';
   evolve_sys_role      CONSTANT        VARCHAR2(30)    := 'EVOLVE';
   trans_etl_role       CONSTANT        VARCHAR2(30)    := 'TRANS_ETL';
   trans_files_role     CONSTANT        VARCHAR2(30)    := 'TRANS_FILES';
   default_repository   CONSTANT        VARCHAR2(6)     := 'TDREP';

   PROCEDURE drop_evolve_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );

   PROCEDURE drop_transcend_repo(
      p_schema       VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );

   PROCEDURE drop_evolve_app(
      p_schema       VARCHAR2
   );

   PROCEDURE drop_transcend_app(
      p_schema       VARCHAR2
   );

   PROCEDURE build_repository(
      p_schema       VARCHAR2,
      p_tablespace   VARCHAR2,
      p_product      VARCHAR2 DEFAULT TRANSCEND_PRODUCT,
      p_drop         BOOLEAN  DEFAULT FALSE
   );
 
   PROCEDURE build_application(
      p_schema       VARCHAR2,
      p_repository   VARCHAR2,
      p_product      VARCHAR2 DEFAULT TRANSCEND_PRODUCT
   );

   PROCEDURE register_user(
      p_user         VARCHAR2,
      p_application  VARCHAR2 DEFAULT DEFAULT_REPOSITORY, 
      p_repository   VARCHAR2 DEFAULT DEFAULT_REPOSITORY
   );
      
   PROCEDURE register_directory (
      p_directory           VARCHAR2,
      p_application         VARCHAR2,
      p_user                VARCHAR2 DEFAULT NULL
   );
   
   PROCEDURE grant_execute_command (
      p_application     VARCHAR2,
      p_user            VARCHAR2   DEFAULT NULL,
      p_name            VARCHAR2   DEFAULT NULL 
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