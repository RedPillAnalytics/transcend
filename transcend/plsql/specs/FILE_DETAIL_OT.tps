CREATE OR REPLACE TYPE file_ot AUTHID CURRENT_USER AS object(
   file_detail_id        NUMBER,                                                           
   file_label            VARCHAR2( 100 ),
   file_group       	 VARCHAR2( 64 ),
   file_type        	 VARCHAR2( 7 ),
   directory        	 VARCHAR2( 30 ),
   filename         	 VARCHAR2( 50 ),
   source_directory 	 VARCHAR2( 50 ),
   source_filename       VARCHAR2( 200 ),
   work_directory   	 VARCHAR2( 30 ),
   lob_type	    	 VARCHAR2( 4 ),
   num_bytes        	 NUMBER,
   num_lines        	 NUMBER,
   file_dt               DATE,
   file_clob             CLOB,
   file_blob             BLOB,
   store_files_native    VARCHAR2( 20 ),
   compress_method	 VARCHAR2( 20 ),
   encrypt_method	 VARCHAR2( 20 ),
   passphrase       	 VARCHAR2( 100 ),
   processed_ts          TIMESTAMP,
   session_id            NUMBER,
   MEMBER PROCEDURE unarchive(
      p_file_detail_id    NUMBER,
      p_directory         VARCHAR2 DEFAULT NULL
   )
)
NOT FINAL;
/