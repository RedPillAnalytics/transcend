CREATE OR REPLACE TYPE dimension_ot UNDER mapping_ot(
   full_table          VARCHAR2( 61 ),
   full_source         VARCHAR2( 61 ),
   sequence_owner      VARCHAR2( 30 ),
   sequence_name       VARCHAR2( 30 ),
   full_sequence       VARCHAR2( 61 ),
   source_owner        VARCHAR2( 30 ),
   source_table        VARCHAR2( 30 ),
   full_stage          VARCHAR2( 61 ),
   named_staging       VARCHAR2( 3 ),
   late_arriving       VARCHAR2( 3 ),
   direct_load         VARCHAR2( 3 ),
   current_ind_col     VARCHAR2( 30 ),
   effect_dt_col       VARCHAR2( 30 ),
   expire_dt_col       VARCHAR2( 30 ),
   surrogate_key_col   VARCHAR2( 30 ),
   natural_key_list    VARCHAR2( 4000 ),
   CONSTRUCTOR FUNCTION dimension_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE verify,
   MEMBER PROCEDURE initialize_cols,
   OVERRIDING MEMBER PROCEDURE confirm_dim_cols,
   OVERRIDING MEMBER PROCEDURE LOAD
);
/