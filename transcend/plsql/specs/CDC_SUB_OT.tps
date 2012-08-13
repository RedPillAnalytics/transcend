
CREATE OR REPLACE TYPE cdc_sub_ot 
AUTHID CURRENT_USER AS object
(
  sub_name               VARCHAR2(30),
  sub_type               VARCHAR2(10),
  group_id               NUMBER,
  fnd_schema             VARCHAR2(30),
  stg_schema             VARCHAR2(30),
  effective_scn          NUMBER,
  expiration_scn         NUMBER,
  
  MEMBER PROCEDURE build_stg_views,
  
  MEMBER PROCEDURE build_fnd_views,

  MEMBER PROCEDURE extend_window
)
NOT FINAL;
/