CREATE OR REPLACE TYPE tdinc.scdhybrid UNDER tdinc.basetype (
   scdhybrid_id        NUMBER,
   scdhybrid_name      VARCHAR2 (100),
   source_owner	       VARCHAR2(30),
   source_object       VARCHAR2(30),
   object_owner        VARCHAR2 (30),
   object_name         VARCHAR2 (30),
   sequence_owner      VARCHAR2 (30),
   sequence_name       VARCHAR2 (30),
   type2_attribs       VARCHAR2(2000),
   type1_attribs       VARCHAR2(2000),
   surrogate_key       VARCHAR(30),
   current_indicator   VARCHAR2(30),
   effective_date      VARCHAR2(30),
   expire_date         VARCHAR2(30),
   MEMBER PROCEDURE process
)
NOT FINAL;
/