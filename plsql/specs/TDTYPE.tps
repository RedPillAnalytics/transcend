CREATE OR REPLACE TYPE tdtype UNDER logtype(
   logging_level   NUMBER,
   CONSTRUCTOR FUNCTION tdtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   OVERRIDING MEMBER PROCEDURE clear_app_info,
   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
)
;
/