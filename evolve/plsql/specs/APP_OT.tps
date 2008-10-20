CREATE OR REPLACE TYPE app_ot AUTHID CURRENT_USER AS object(
   prev_client_info        VARCHAR2( 64 ),
   prev_module             VARCHAR2( 48 ),
   prev_action             VARCHAR2( 32 ),
   prev_registration	   VARCHAR2( 20 ),
   prev_logging_level	   NUMBER,
   prev_runmode	           VARCHAR2( 10 ),
   prev_consistent_name	   VARCHAR2( 3 ),
   prev_batch_id	   NUMBER,
   CONSTRUCTOR FUNCTION app_ot(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2,
   MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info,    
   MEMBER PROCEDURE read_prev_info
)
NOT FINAL;
/