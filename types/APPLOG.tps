CREATE OR REPLACE TYPE tdinc.applog AS OBJECT(
   instance_name      VARCHAR2( 30 ),
   session_id         NUMBER,
   machine            VARCHAR2( 50 ),
   dbuser             VARCHAR2( 30 ),
   osuser             VARCHAR2( 30 ),
   client_info        VARCHAR2( 64 ),
   module             VARCHAR2( 48 ),
   action             VARCHAR2( 32 ),
   prev_client_info   VARCHAR2( 64 ),
   prev_module        VARCHAR2( 48 ),
   prev_action        VARCHAR2( 32 ),
   CONSTRUCTOR FUNCTION applog(
      p_action        VARCHAR2 DEFAULT 'Begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_register      BOOLEAN DEFAULT TRUE,
      p_debug         BOOLEAN DEFAULT FALSE )
      RETURN SELF AS RESULT,
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2,
   MEMBER FUNCTION whence
      RETURN VARCHAR2,
   MEMBER PROCEDURE set_action(
      p_action   VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info,
   MEMBER PROCEDURE log_msg(
      p_msg   VARCHAR2 ),
   MEMBER PROCEDURE log_err,
   MEMBER PROCEDURE log_cnt(
      p_count   NUMBER ),
   MEMBER PROCEDURE log_cnt_msg(
      p_count   NUMBER,
      p_msg     VARCHAR2 DEFAULT NULL )
);
/