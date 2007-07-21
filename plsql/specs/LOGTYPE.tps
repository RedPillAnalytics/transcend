CREATE OR REPLACE TYPE logtype UNDER apptype(
   instance_name   VARCHAR2( 30 ),
   session_id      NUMBER,
   machine         VARCHAR2( 50 ),
   dbuser          VARCHAR2( 30 ),
   osuser          VARCHAR2( 30 ),
   CONSTRUCTOR FUNCTION logtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_register      VARCHAR2 DEFAULT 'yes',
      p_runmode       VARCHAR2 DEFAULT 'runtime'
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE set_session_info,
   MEMBER FUNCTION whence
      RETURN VARCHAR2,
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
   )
)
NOT FINAL;
/