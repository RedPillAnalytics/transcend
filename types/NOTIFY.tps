CREATE OR REPLACE TYPE tdinc.notify UNDER tdinc.basetype (
   notify_id       NUMBER,
   notify_method   VARCHAR2 (20),
   action          VARCHAR2 (32),
   module          VARCHAR2 (48),
   module_id       NUMBER,
   MESSAGE         VARCHAR2 (2000),
   subject         VARCHAR2 (100),
   MEMBER PROCEDURE send
)
NOT FINAL;
/