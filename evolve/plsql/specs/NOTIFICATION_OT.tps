CREATE OR REPLACE TYPE notification_ot AS OBJECT(
   label        VARCHAR2( 40 ),
   event_name   VARCHAR2( 30 ),
   module       VARCHAR2( 48 ),
   action       VARCHAR2( 32 ),
   method       VARCHAR2( 20 ),
   enabled      VARCHAR( 3 ),
   required     VARCHAR2( 3 ),
   subject      VARCHAR2( 100 ),
   MESSAGE      VARCHAR2( 2000 ),
   sender       VARCHAR2( 1024 ),
   recipients   VARCHAR2( 2000 ),
   CONSTRUCTOR FUNCTION notification_ot( p_label VARCHAR2 )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE send( p_message VARCHAR2 DEFAULT NULL )
)
NOT FINAL;
/