CREATE OR REPLACE TYPE dimension_ot FORCE 
UNDER mapping_ot
(
   full_table          VARCHAR2( 61 ),
   full_source         VARCHAR2( 61 ),
   sequence_owner      VARCHAR2( 30 ),
   sequence_name       VARCHAR2( 30 ),
   full_sequence       VARCHAR2( 61 ),
   source_owner        VARCHAR2( 30 ),
   source_table        VARCHAR2( 30 ),
   full_stage          VARCHAR2( 61 ),
   named_staging       VARCHAR2( 3 ),
   direct_load         VARCHAR2( 3 ),
   current_ind_col     VARCHAR2( 30 ),
   effect_dt_col       VARCHAR2( 30 ),
   expire_dt_col       VARCHAR2( 30 ),
   surrogate_key_col   VARCHAR2( 30 ),
   natural_key_list    VARCHAR2( 4000 ),
   scd1_list           VARCHAR2( 4000 ),
   statement           VARCHAR2(32000),
   nonscd_statement    VARCHAR2(32000),

   CONSTRUCTOR FUNCTION dimension_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT,
                                                   
   OVERRIDING MEMBER PROCEDURE verify,
                                                   
   MEMBER PROCEDURE initialize_cols,
                                                   
   MEMBER PROCEDURE confirm_dim_cols,
                                                   
   MEMBER PROCEDURE create_source_table,
  
   MEMBER PROCEDURE drop_source_table,

   MEMBER PROCEDURE create_staging_table,
  
   MEMBER PROCEDURE drop_staging_table,
                                                   
   MEMBER PROCEDURE load_staging,
                                                   
   OVERRIDING MEMBER PROCEDURE replace_table,
                                                   
   OVERRIDING MEMBER PROCEDURE pre_map,
                                                   
   OVERRIDING MEMBER PROCEDURE post_map,

   OVERRIDING MEMBER PROCEDURE post_verify,
  
   OVERRIDING MEMBER PROCEDURE post_create,

   OVERRIDING MEMBER PROCEDURE post_delete

);
/