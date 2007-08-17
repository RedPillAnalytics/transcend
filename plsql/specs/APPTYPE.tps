CREATE OR REPLACE TYPE apptype AUTHID CURRENT_USER AS object(
   prev_client_info        VARCHAR2( 64 ),
   prev_module             VARCHAR2( 48 ),
   prev_action             VARCHAR2( 32 ),
   prev_registration	   VARCHAR2( 20 ),
   prev_logging_level	   NUMBER,
   prev_runmode	           VARCHAR2( 10 ),
   prev_batch_id	   NUMBER,					    
   CONSTRUCTOR FUNCTION apptype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2,
   MEMBER FUNCTION is_debugmode
      RETURN BOOLEAN,
   MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info,    
   MEMBER PROCEDURE read_prev_info,
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN,
   MEMBER FUNCTION have_runmode_priority ( p_attribute VARCHAR2 )
     RETURN BOOLEAN,
   MEMBER FUNCTION have_logging_priority ( p_attribute VARCHAR2 )
     RETURN BOOLEAN,
   MEMBER FUNCTION have_registration_priority ( p_attribute VARCHAR2 )
     RETURN BOOLEAN
)
NOT FINAL;
/