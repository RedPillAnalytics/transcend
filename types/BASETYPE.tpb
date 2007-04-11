CREATE OR REPLACE TYPE BODY tdinc.basetype
AS
   -- GET method for DEBUG mode
   MEMBER FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE runmode
         WHEN 'debug'
            THEN TRUE
         ELSE FALSE
      END;
   END is_debugmode;
   -- SET method for DEBUG mode
   MEMBER PROCEDURE set_runmode (p_runmode VARCHAR2)
   AS
   BEGIN
      runmode := CASE
                   WHEN REGEXP_LIKE (p_runmode, 'debug', 'i')
                      THEN 'debug'
                   ELSE 'runtime'
                END;
   END set_runmode;
END;
/