-- GET method for pulling an error code out of the ERR_CD table

CREATE OR REPLACE FUNCTION get_err_cd( p_name VARCHAR2 )
   RETURN NUMBER
AS
   l_code   err_cd.code%TYPE;
BEGIN
   SELECT code
     INTO l_code
     FROM err_cd
    WHERE NAME = p_name;

   RETURN l_code;
END get_err_cd;
/