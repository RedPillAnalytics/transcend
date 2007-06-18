CREATE OR REPLACE TYPE email UNDER notify (
   sender       VARCHAR2 (1024),
   recipients   VARCHAR2 (2000),
   MEMBER PROCEDURE send
)
NOT FINAL;
/