CREATE OR REPLACE TYPE emailtype UNDER notifytype (
   sender       VARCHAR2 (1024),
   recipients   VARCHAR2 (2000),
   MEMBER PROCEDURE send
)
NOT FINAL;
/