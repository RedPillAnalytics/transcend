CREATE OR REPLACE TYPE extract_ot UNDER file_ot(
   dateformat_ddl   VARCHAR2( 250 ),
   tsformat_ddl     VARCHAR2( 250 ),
   delimiter        VARCHAR2( 1 ),
   quotechar        VARCHAR2( 1 ),
   headers          VARCHAR2( 3 ),
   MEMBER PROCEDURE process
)
;
/