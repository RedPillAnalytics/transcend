CREATE OR REPLACE TYPE tdinc.applog UNDER tdinc.basetype (
   instance_name      VARCHAR2 (30),
   session_id         NUMBER,
   machine            VARCHAR2 (50),
   dbuser             VARCHAR2 (30),
   osuser             VARCHAR2 (30),
   client_info        VARCHAR2 (64),
   module             VARCHAR2 (48),
   action             VARCHAR2 (32),
   registration       VARCHAR2 (20),
   logging_level      NUMBER,
   prev_client_info   VARCHAR2 (64),
   prev_module        VARCHAR2 (48),
   prev_action        VARCHAR2 (32),
   CONSTRUCTOR FUNCTION applog (
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL)
      RETURN SELF AS RESULT,
   MEMBER FUNCTION whence
      RETURN VARCHAR2,
   MEMBER PROCEDURE set_action (p_action VARCHAR2),
   MEMBER PROCEDURE clear_app_info,
   MEMBER PROCEDURE log_msg (
      p_msg      VARCHAR2,
      p_level    NUMBER DEFAULT 2,
      p_stdout   VARCHAR2 DEFAULT 'yes'),
   MEMBER PROCEDURE log_err,
   MEMBER PROCEDURE log_cnt_msg (p_count NUMBER, p_msg VARCHAR2 DEFAULT NULL),
   MEMBER FUNCTION get_err_cd (p_name VARCHAR2)
      RETURN NUMBER,
   MEMBER FUNCTION get_err_msg (p_name VARCHAR2)
      RETURN VARCHAR2,
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN,
   MEMBER FUNCTION get_value_vchr (p_name VARCHAR2)
      RETURN VARCHAR2,
   MEMBER FUNCTION get_value_num (p_name VARCHAR2)
      RETURN NUMBER,
   MEMBER PROCEDURE send (
      p_module_id   number,
      p_message     VARCHAR2 DEFAULT null)
)
;
/