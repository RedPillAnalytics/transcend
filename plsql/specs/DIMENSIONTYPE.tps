CREATE OR REPLACE TYPE dimensiontype AUTHID CURRENT_USER AS object
( owner	             VARCHAR2( 30 ),
  table_name  	     VARCHAR2( 30 ),
  full_table	     VARCHAR2( 61 ),
  source_owner	     VARCHAR2( 30 ),
  source_object      VARCHAR2( 30 ),
  full_source 	     VARCHAR2( 61 ),
  replace_method     VARCHAR2( 10 ),
  select_statment    VARCHAR2(4000),
  member PROCEDURE load
)
;
/