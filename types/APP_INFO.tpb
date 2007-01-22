-- object for holding previous values of DBMS_APPLICATION_INFO attributes to set them back later 
CREATE OR REPLACE TYPE BODY efw.app_info
AS
   CONSTRUCTOR FUNCTION app_info(
      p_action        VARCHAR2 DEFAULT 'Begin Procedure/Function',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_debug         BOOLEAN DEFAULT FALSE )
      RETURN SELF AS RESULT
   AS
   BEGIN
      DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
      DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      DBMS_APPLICATION_INFO.set_client_info( NVL( p_client_info, prev_client_info ));
      DBMS_APPLICATION_INFO.set_module( ( CASE
                                             WHEN     p_debug
                                                  AND p_module IS NOT NULL
                                                THEN p_module || ' (DEBUG)'
                                             WHEN     NOT p_debug
                                                  AND p_module IS NOT NULL
                                                THEN p_module
                                             WHEN     p_debug
                                                  AND p_module IS NULL
                                                THEN utility.get_package_name || ' (DEBUG)'
                                             ELSE utility.get_package_name
                                          END ),
                                        p_action );
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END app_info;
   MEMBER PROCEDURE set_action(
      p_action   VARCHAR2 )
   AS
   BEGIN
      DBMS_APPLICATION_INFO.set_action( p_action );
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END set_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      DBMS_APPLICATION_INFO.set_client_info( prev_client_info );
      DBMS_APPLICATION_INFO.set_module( prev_module, prev_action );
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END clear_app_info;
END;
/