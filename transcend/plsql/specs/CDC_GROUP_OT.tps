
CREATE OR REPLACE TYPE cdc_group_ot force
AUTHID CURRENT_USER AS object
(
  source_type            VARCHAR2(10),
  dblink_name            VARCHAR2(30),
  group_id               NUMBER,
  group_name             VARCHAR2(30),
  initial_source_scn     NUMBER,
  foundation             VARCHAR2(30),
  subscription           VARCHAR2(30),
  sub_prefix             VARCHAR2(4),
  filter_policy          VARCHAR2(20),
  source_scn             VARCHAR2(30),
  commit_date            VARCHAR2(30),
  source_minscn          VARCHAR2(30),
  source_maxscn          VARCHAR2(30),
  row_rank               VARCHAR2(30),
  cdc_rank               VARCHAR2(30),
  entity_rank            VARCHAR2(30),
  dml_type               VARCHAR2(30),
  
  CONSTRUCTOR FUNCTION cdc_group_ot 
  ( 
    p_group_name         VARCHAR2
  )
  RETURN SELF AS RESULT,

  MEMBER PROCEDURE initialize
   ( 
     p_group_name VARCHAR2
   ),
  
   MEMBER PROCEDURE register_initial_scn
   ( 
     p_scn        NUMBER
   ),
  
  MEMBER FUNCTION get_source_scn
  RETURN NUMBER,

  MEMBER FUNCTION get_entityrank_clause
  (
    p_natkey  VARCHAR2
  )
  RETURN VARCHAR2,

  MEMBER FUNCTION get_cdcrank_clause
  (
    p_natkey  VARCHAR2
  )
  RETURN VARCHAR2,
  
  MEMBER FUNCTION get_expiration_clause
  (
    p_natkey  VARCHAR2
  )
  RETURN VARCHAR2,

  MEMBER FUNCTION get_join_select
  (
    p_table    VARCHAR2,
    p_natkey   VARCHAR2,
    p_collist  VARCHAR2,
    p_collapse BOOLEAN
  )
  RETURN VARCHAR2,

   MEMBER FUNCTION get_case_select
   (
    p_table    VARCHAR2,
    p_natkey   VARCHAR2,
    p_collist  VARCHAR2,
    p_collapse BOOLEAN
   )
   RETURN VARCHAR2,

   MEMBER FUNCTION get_ctas_statement
   (
     p_owner           VARCHAR2,
     p_table           VARCHAR2,
     p_source_owner    VARCHAR2,
     p_source_table    VARCHAR2,
     p_dblink          VARCHAR2,
     p_collist         VARCHAR2 DEFAULT NULL,
     p_rows            BOOLEAN DEFAULT TRUE
   )
   RETURN VARCHAR2,

  MEMBER PROCEDURE build_view
  (
    p_table   VARCHAR2,
    p_natkey  VARCHAR2
  ),
                                    
   MEMBER PROCEDURE build_table
   (
     p_owner            VARCHAR2,
     p_table            VARCHAR2,
     p_source_owner     VARCHAR2,
     p_source_table     VARCHAR2,
     p_natkey           VARCHAR2,
     p_add_rows         BOOLEAN  DEFAULT FALSE,
     p_dblink           VARCHAR2 DEFAULT NULL
   ),
                                    
   MEMBER PROCEDURE add_audit_columns
   ( 
     p_owner            VARCHAR2,
     p_table            VARCHAR2,
     p_scn              NUMBER    DEFAULT 0,
     p_dmltype          VARCHAR2  DEFAULT 'initial load',
     p_commit_date      DATE      DEFAULT SYSDATE,
     p_rowrank          NUMBER    DEFAULT 1
   ),

   MEMBER PROCEDURE build_subscription,

   MEMBER PROCEDURE build_foundation
   (
     p_scn      NUMBER          DEFAULT NULL
   ),
                                   
   MEMBER PROCEDURE load_foundation
  
)
NOT FINAL
/