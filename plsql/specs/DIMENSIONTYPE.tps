CREATE OR REPLACE TYPE dimensiontype AUTHID CURRENT_USER AS object
( owner	             VARCHAR2( 30 ),
  table_name  	     VARCHAR2( 30 ),
  source_owner	     VARCHAR2( 30 ),
  source_object      VARCHAR2( 30 ),
  replace_method     VARCHAR2( 10 ),
  select_statment    VARCHAR2(4000),
  member PROCEDURE index_maint,
  member PROCEDURE constraint_maint,
  member PROCEDURE replace_data
)
;
/