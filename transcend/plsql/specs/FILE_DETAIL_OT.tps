CREATE OR REPLACE TYPE file_detail_ot AUTHID CURRENT_USER AS object(
   file_detail_id        NUMBER,                                                           
   file_label            VARCHAR2( 100 ),
   file_group       	 VARCHAR2( 64 ),
   file_type        	 VARCHAR2( 7 ),
   directory        	 VARCHAR2( 30 ),
   filename         	 VARCHAR2( 50 ),
   source_directory 	 VARCHAR2( 50 ),
   source_filename       VARCHAR2( 200 ),
   num_bytes        	 NUMBER,
   num_lines        	 NUMBER,
   file_dt               DATE,
   store_original_files  VARCHAR2( 20 ),
   compress_method	 VARCHAR2( 20 ),
   encrypt_method	 VARCHAR2( 20 ),
   passphrase       	 VARCHAR2( 100 ),
   label_file            BLOB,
   processed_ts          TIMESTAMP,
   session_id            NUMBER,
   CONSTRUCTOR FUNCTION file_detail_ot ( 
      p_file_detail_id   NUMBER,
      p_directory        VARCHAR2 DEFAULT NULL 
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE verify,
   MEMBER PROCEDURE inspect(
      p_max_bytes   NUMBER,
      p_min_bytes   NUMBER
   ),
   MEMBER PROCEDURE unarchive
)
NOT FINAL;
/