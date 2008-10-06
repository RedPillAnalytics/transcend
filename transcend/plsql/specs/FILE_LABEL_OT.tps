CREATE OR REPLACE TYPE file_label_ot AUTHID CURRENT_USER AS object(
   file_label            VARCHAR2( 100 ),
   file_group       	 VARCHAR2( 64 ),
   label_type        	 VARCHAR2( 7 ),
   object_owner     	 VARCHAR2( 30 ),
   object_name      	 VARCHAR2( 30 ),
   directory        	 VARCHAR2( 30 ),
   filename         	 VARCHAR2( 50 ),
   source_directory 	 VARCHAR2( 50 ),
   work_directory   	 VARCHAR2( 30 ),
   min_bytes        	 NUMBER,
   max_bytes        	 NUMBER,
   reject_limit          NUMBER,
   baseurl          	 VARCHAR2( 2000 ),
   store_original_files    VARCHAR2( 20 ),
   compress_method	 VARCHAR2( 20 ),
   encrypt_method	 VARCHAR2( 20 ),
   passphrase       	 VARCHAR2( 100 ),
   MEMBER FUNCTION archive(
      p_loc_directory      VARCHAR2,
      p_loc_filename       VARCHAR2,
      p_directory          VARCHAR2,
      p_filename           VARCHAR2,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_filename    VARCHAR2 DEFAULT NULL,
      p_file_dt            DATE     DEFAULT NULL
   )
      RETURN NUMBER,
   MEMBER PROCEDURE modify_archive(
      p_file_detail_id     NUMBER,
      p_loc_directory      VARCHAR2,
      p_loc_filename       VARCHAR2,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_filename    VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_file_dt            DATE     DEFAULT NULL
   ),
   MEMBER PROCEDURE audit_object ( p_file_detail_id NUMBER ),
   MEMBER PROCEDURE announce(
      p_files_url        VARCHAR2,
      p_num_lines   	 NUMBER,
      p_num_files   	 NUMBER DEFAULT 1
   ),
   NOT instantiable MEMBER PROCEDURE verify,
   NOT instantiable MEMBER PROCEDURE process
)
NOT FINAL NOT INSTANTIABLE
/