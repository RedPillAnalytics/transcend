-- GET method for pulling error text out of the ERR_CD table

CREATE OR REPLACE FUNCTION get_err_msg( p_name VARCHAR2 )
   RETURN VARCHAR2
AS
   l_msg   err_cd.MESSAGE%TYPE;
BEGIN
   SELECT MESSAGE
     INTO l_msg
     FROM err_cd
    WHERE NAME = p_name;

   RETURN l_msg;
END get_err_msg;