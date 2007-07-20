CREATE OR REPLACE TYPE tdtype UNDER apptype(
   instance_name   VARCHAR2( 30 ),
   session_id      NUMBER,
   machine         VARCHAR2( 50 ),
   dbuser          VARCHAR2( 30 ),
   osuser          VARCHAR2( 30 ),
   registration    VARCHAR2( 20 ),
   logging_level   NUMBER,
   CONSTRUCTOR FUNCTION tdtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   MEMBER FUNCTION whence
      RETURN VARCHAR2,
   OVERRIDING MEMBER PROCEDURE set_action( p_action VARCHAR2 ),
   OVERRIDING MEMBER PROCEDURE clear_app_info,
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN,
   MEMBER PROCEDURE log_msg(
      p_msg       VARCHAR2,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   ),
   MEMBER PROCEDURE log_err,
   MEMBER PROCEDURE log_cnt_msg(
      p_count     NUMBER,
      p_msg       VARCHAR2 DEFAULT NULL,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   ),
   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
)
;
/