CREATE OR REPLACE PACKAGE BODY td_core
AS
   -- returns a boolean
   -- accepts a varchar2 and determines if regexp matches 'yes' or 'no'
   -- raises an error if it doesn't
   FUNCTION is_true( p_parm VARCHAR2, p_allownulls BOOLEAN DEFAULT FALSE )
      RETURN BOOLEAN
   AS
   BEGIN
      -- use the load_tab or merge_tab procedure depending on P_MERGE
      CASE
         WHEN REGEXP_LIKE( 'yes', p_parm, 'i' )
         THEN
            RETURN TRUE;
         WHEN REGEXP_LIKE( 'no', p_parm, 'i' )
         THEN
            RETURN FALSE;
         ELSE
            IF p_parm IS NULL AND p_allownulls
            THEN
               RETURN NULL;
            ELSE
               raise_application_error
                                 ( -20030,
                                      'The specified parameter value is not recognized: '
                                   || p_parm
                                 );
            END IF;
      END CASE;
   END is_true;

   -- much like IS_TRUE above, but BOOLEANS, though useful in PL/SQL, are not supported in SQL
   -- this can be used in SQL cursors
   -- returns a varchar2
   -- accepts a varchar2 and determines if regexp matches 'yes' or 'no'
   -- raises an error if it doesn't
   FUNCTION get_yn_ind( p_parm VARCHAR2 )
      RETURN VARCHAR2
   AS
   BEGIN
      -- use the load_tab or merge_tab procedure depending on P_MERGE
      CASE
         WHEN REGEXP_LIKE( 'yes', p_parm, 'i' )
         THEN
            RETURN 'yes';
         WHEN REGEXP_LIKE( 'no', p_parm, 'i' )
         THEN
            RETURN 'no';
         ELSE
            raise_application_error
                                 ( -20030,
                                      'The specified parameter value is not recognized: '
                                   || p_parm
                                 );
      END CASE;
   END get_yn_ind;

   -- function takes a text string and a delimiter and parses the string
   -- should only be used as a pipelined table function
   FUNCTION SPLIT( p_text VARCHAR2, p_delimiter VARCHAR2 := ',' )
      RETURN t_split PIPELINED
   IS
      l_idx     PLS_INTEGER;
      l_list    VARCHAR2( 32767 ) := p_text;
      l_value   VARCHAR2( 32767 );
   BEGIN
      LOOP
         l_idx := INSTR( l_list, p_delimiter );

         IF l_idx > 0
         THEN
            PIPE ROW( SUBSTR( l_list, 1, l_idx - 1 ));
            l_list := SUBSTR( l_list, l_idx + LENGTH( p_delimiter ));
         ELSE
            PIPE ROW( l_list );
            EXIT;
         END IF;
      END LOOP;

      RETURN;
   END SPLIT;
END td_core;
/

SHOW errors