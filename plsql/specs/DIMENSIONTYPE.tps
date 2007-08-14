CREATE OR REPLACE TYPE dimensiontype UNDER basetype(
   owner	      VARCHAR2( 30 ),
   table_name	      VARCHAR2( 30 ),
   source_owner	      VARCHAR2( 30 ),
   source_object      VARCHAR2( 30 ),
   replace_method     VARCHAR2( 10 ),
   select_statment    VARCHAR2(4000)
)
;
/