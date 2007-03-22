CREATE OR REPLACE TYPE tdinc.fhconf UNDER tdinc.notify (
   filehub_id       NUMBER,
   filehub_name     VARCHAR2 (100),
   filehub_group    VARCHAR2 (64),
   filehub_type     VARCHAR2 (7),
   object_owner     VARCHAR2 (30),
   object_name      VARCHAR2 (30),
   DIRECTORY        VARCHAR2 (30),
   filename         VARCHAR2 (50),
   filepath         VARCHAR2 (100),
   arch_directory   VARCHAR2 (30),
   arch_filename    VARCHAR2 (50),
   arch_filepath    VARCHAR2 (100),
   min_bytes        NUMBER,
   max_bytes        NUMBER,
   file_url         VARCHAR2 (2000),
   CONSTRUCTOR FUNCTION fhconf (p_debug BOOLEAN DEFAULT FALSE)
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE audit_file (
      p_src_filename    VARCHAR2 DEFAULT NULL,
      p_num_bytes       NUMBER,
      p_num_lines       NUMBER,
      p_file_dt         DATE)
)
NOT FINAL;
/