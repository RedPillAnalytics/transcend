CREATE OR REPLACE TYPE emailtype UNDER notify_ot (
   sender       VARCHAR2 (1024),
   recipients   VARCHAR2 (2000),
   MEMBER PROCEDURE send
)
NOT FINAL;
/