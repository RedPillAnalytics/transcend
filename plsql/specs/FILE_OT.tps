CREATE OR REPLACE TYPE file_ot AUTHID CURRENT_USER AS object(
   file_label       VARCHAR2( 100 ),
   file_group       VARCHAR2( 64 ),
   file_type        VARCHAR2( 7 ),
   object_owner     VARCHAR2( 30 ),
   object_name      VARCHAR2( 30 ),
   directory        VARCHAR2( 30 ),
   dirpath          VARCHAR2( 200 ),
   filename         VARCHAR2( 50 ),
   filepath         VARCHAR2( 100 ),
   arch_directory   VARCHAR2( 30 ),
   arch_dirpath     VARCHAR( 200 ),
   arch_filename    VARCHAR2( 50 ),
   arch_filepath    VARCHAR2( 100 ),
   file_datestamp   VARCHAR2( 30 ),
   min_bytes        NUMBER,
   max_bytes        NUMBER,
   baseurl          VARCHAR2( 2000 ),
   file_url         VARCHAR2( 2000 ),
   passphrase       VARCHAR2( 100 ),
   MEMBER PROCEDURE audit_file(
      p_filepath          VARCHAR2,
      p_source_filepath   VARCHAR2,
      p_arch_filepath     VARCHAR2,
      p_num_bytes         NUMBER,
      p_num_lines         NUMBER,
      p_file_dt           DATE,
      p_validate          VARCHAR2 DEFAULT 'yes'
   ),
   MEMBER PROCEDURE audit_file(
      p_num_bytes   NUMBER,
      p_num_lines   NUMBER,
      p_file_dt     DATE,
      p_validate    VARCHAR2 DEFAULT 'yes'
   )
)
NOT FINAL;
/