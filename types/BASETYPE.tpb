CREATE OR REPLACE TYPE BODY tdinc.basetype
AS
   -- GET method for DEBUG mode
   MEMBER FUNCTION DEBUG_MODE
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE DEBUG
         WHEN 'Y'
            THEN TRUE
         ELSE FALSE
      END;
   END DEBUG_MODE;
   -- SET method for DEBUG mode
   MEMBER PROCEDURE DEBUG_MODE (p_debug BOOLEAN DEFAULT FALSE)
   AS
   BEGIN
      DEBUG := CASE p_debug
                 WHEN TRUE
                    THEN 'Y'
                 ELSE 'F'
              END;
   END DEBUG_MODE;
END;
/