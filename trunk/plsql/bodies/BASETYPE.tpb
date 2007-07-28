CREATE OR REPLACE TYPE BODY basetype
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
   -- SET method for RUNMODE
   MEMBER PROCEDURE set_runmode( p_runmode VARCHAR2 )
   AS
   BEGIN
      CASE
         WHEN REGEXP_LIKE( 'debug', '^' || NVL( p_runmode, '^\W$' ), 'i' )
         THEN
            runmode := 'debug';
         WHEN REGEXP_LIKE( 'runtime', '^' || NVL( p_runmode, '^\W$' ), 'i' )
         THEN
            runmode := 'runtime';
         ELSE
            raise_application_error
                                ( -20022,
                                     'The specified parameter value is not recognized : '
                                  || p_runmode
                                );
      END CASE;
   END set_runmode;
END;
/