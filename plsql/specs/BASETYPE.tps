CREATE OR REPLACE TYPE tdinc.basetype AS OBJECT (
   runmode   VARCHAR2 (10),
   MEMBER FUNCTION is_debugmode
      RETURN BOOLEAN,
   MEMBER PROCEDURE set_runmode (p_runmode VARCHAR2)
)
NOT FINAL;
/