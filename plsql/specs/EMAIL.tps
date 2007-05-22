CREATE OR REPLACE TYPE tdinc.email UNDER tdinc.notify (
   sender       VARCHAR2 (1024),
   recipients   VARCHAR2 (2000),
   MEMBER PROCEDURE send
)
NOT FINAL;
/