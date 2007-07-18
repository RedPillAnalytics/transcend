CREATE OR REPLACE TYPE apptype AS OBJECT(
   client_info        VARCHAR2( 64 ),
   module             VARCHAR2( 48 ),
   action             VARCHAR2( 32 ),
   prev_client_info   VARCHAR2( 64 ),
   prev_module        VARCHAR2( 48 ),
   prev_action        VARCHAR2( 32 ),
   registration       VARCHAR2( 20 ),
   runmode            VARCHAR2( 10 ),
   CONSTRUCTOR FUNCTION apptype(
      p_action         VARCHAR2 DEFAULT 'begin module',
      p_module         VARCHAR2 DEFAULT NULL,
      p_client_info    VARCHAR2 DEFAULT NULL,
      p_registration   VARCHAR2 DEFAULT 'register',
      p_runmode        VARCHAR2 DEFAULT 'runtime'
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE set_action( p_action VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info,
   MEMBER PROCEDURE set_registration( p_registration VARCHAR2 ),
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN,
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2,
   MEMBER PROCEDURE set_runmode( p_runmode VARCHAR2 ),
   MEMBER FUNCTION is_debugmode
      RETURN BOOLEAN
)
NOT FINAL;
/