CREATE OR REPLACE TYPE tdtype UNDER apptype(
   CONSTRUCTOR FUNCTION tdtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
)
;
/