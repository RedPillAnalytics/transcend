CREATE OR REPLACE TYPE tdtype UNDER apptype(
   prev_client_info        VARCHAR2( 64 ),
   prev_module             VARCHAR2( 48 ),
   prev_action             VARCHAR2( 32 ),
   prev_runmode	     VARCHAR2( 10 ),
   prev_registration	     VARCHAR2( 20 ),
   prev_logging_level	     NUMBER,
   CONSTRUCTOR FUNCTION tdtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   OVERRIDING MEMBER PROCEDURE log_msg(
      p_msg       VARCHAR2,
      p_level     NUMBER DEFAULT 2,
      p_stdout    VARCHAR2 DEFAULT 'yes',
      p_oper_id   NUMBER DEFAULT NULL
   ),
   OVERRIDING MEMBER PROCEDURE clear_app_info,
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN,
   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
)
;
/