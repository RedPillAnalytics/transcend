CREATE OR REPLACE TYPE mapping_ot
   AUTHID CURRENT_USER
AS OBJECT
(
  mapping_name              VARCHAR2( 30 ),
  mapping_type		    VARCHAR2( 10 ), 
  table_owner               VARCHAR2( 61 ),
  table_name                VARCHAR2( 30 ),
  partition_name            VARCHAR2( 30 ),
  manage_indexes            VARCHAR2( 7 ),
  index_regexp              VARCHAR2( 30 ),
  index_type                VARCHAR2( 30 ),
  partition_type            VARCHAR2( 30 ),
  index_concurrency         VARCHAR2( 3 ),
  manage_constraints        VARCHAR2( 7 ),
  constraint_regexp         VARCHAR2( 100 ),
  constraint_type           VARCHAR2( 100 ),
  constraint_concurrency    VARCHAR2( 3 ),
  drop_dependent_objects    VARCHAR2( 3 ),
  staging_owner             VARCHAR2( 30 ),
  staging_table             VARCHAR2( 30 ),
  staging_column            VARCHAR2( 30 ),
  replace_method            VARCHAR2( 10 ),
  statistics                VARCHAR2( 10 ),
  CONSTRUCTOR FUNCTION mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
     RETURN SELF AS RESULT,
  MEMBER PROCEDURE register ( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL ),
  MEMBER PROCEDURE verify,
  MEMBER PROCEDURE unusable_indexes,
  MEMBER PROCEDURE disable_constraints,
  MEMBER PROCEDURE pre_map,
  MEMBER PROCEDURE replace_table,
  MEMBER PROCEDURE usable_indexes,
  MEMBER PROCEDURE enable_constraints,
  MEMBER PROCEDURE gather_stats,
  MEMBER PROCEDURE post_map,
  MEMBER PROCEDURE start_map,
  MEMBER PROCEDURE end_map,
  MEMBER PROCEDURE confirm_dim_cols
)
NOT FINAL;
/