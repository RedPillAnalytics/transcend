CREATE OR REPLACE TYPE extract_ot UNDER file_label_ot(
   file_datestamp   VARCHAR2( 30 ),
   dateformat_ddl   VARCHAR2( 250 ),
   tsformat_ddl     VARCHAR2( 250 ),
   delimiter        VARCHAR2( 1 ),
   quotechar        VARCHAR2( 1 ),
   headers          VARCHAR2( 3 ),
   file_url	    VARCHAR2( 1000 ),
   CONSTRUCTOR FUNCTION extract_ot(
      p_file_label   VARCHAR2,
      p_directory    VARCHAR2 DEFAULT NULL 
   )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE verify,
   OVERRIDING MEMBER PROCEDURE process
)
;
/