CREATE OR REPLACE TYPE dimension_ot AUTHID CURRENT_USER AS object
( owner	             VARCHAR2( 30 ),
  table_name  	     VARCHAR2( 30 ),
  full_table	     VARCHAR2( 61 ),
  source_owner	     VARCHAR2( 30 ),
  source_object      VARCHAR2( 30 ),
  full_source 	     VARCHAR2( 61 ),
  staging_owner      VARCHAR2( 30 ),
  staging_table	     VARCHAR2( 30 ),
  full_stage	     VARCHAR2( 61 ),
  constant_staging   VARCHAR2( 3 ),
  direct_load	     VARCHAR2( 3 ),
  replace_method     VARCHAR2( 10 ),
  statistics	     VARCHAR2( 10 ),
  load_sql           VARCHAR2(4000),
   CONSTRUCTOR FUNCTION dimension_ot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN SELF AS RESULT,
  member PROCEDURE load
)
;
/