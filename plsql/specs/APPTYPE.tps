CREATE OR REPLACE TYPE apptype UNDER basetype(
   client_info        VARCHAR2( 64 ),
   module             VARCHAR2( 48 ),
   action             VARCHAR2( 32 ),
   prev_client_info   VARCHAR2( 64 ),
   prev_module        VARCHAR2( 48 ),
   prev_action        VARCHAR2( 32 ),
   REGISTER           VARCHAR2( 3 ),
   CONSTRUCTOR FUNCTION apptype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_register      VARCHAR2 DEFAULT 'yes',
      p_runmode       VARCHAR2 DEFAULT 'runtime'
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE set_module( p_module VARCHAR2 ),
   MEMBER PROCEDURE set_action( p_action VARCHAR2 ),
   MEMBER PROCEDURE set_register( p_register VARCHAR2 ),
   MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info,
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2
)
NOT FINAL;
/