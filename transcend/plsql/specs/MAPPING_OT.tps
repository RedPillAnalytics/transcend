CREATE OR REPLACE TYPE mapping_ot
   AUTHID CURRENT_USER
AS OBJECT(
   mapping_name         VARCHAR2( 30 ),
   mapping_type		VARCHAR2( 10 ), 
   table_owner          VARCHAR2( 61 ),
   table_name           VARCHAR2( 30 ),
   partition_name       VARCHAR2( 30 ),
   manage_indexes       VARCHAR2( 3 ),
   manage_constraints   VARCHAR2( 3 ),
   source_owner         VARCHAR2( 30 ),
   source_object        VARCHAR2( 30 ),
   source_column        VARCHAR2( 30 ),
   replace_method       VARCHAR2( 10 ),
   STATISTICS           VARCHAR2( 10 ),
   concurrent           VARCHAR2( 3 ),
   index_regexp         VARCHAR2( 30 ),
   index_type           VARCHAR2( 30 ),
   partition_type       VARCHAR2( 30 ),
   constraint_regexp    VARCHAR2( 100 ),
   constraint_type      VARCHAR2( 100 ),
   CONSTRUCTOR FUNCTION mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE register ( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL ),
   MEMBER PROCEDURE verify,
   MEMBER PROCEDURE start_map,
   MEMBER PROCEDURE end_map,
   MEMBER PROCEDURE load,
   MEMBER PROCEDURE confirm_dim_cols
)
NOT FINAL;
/