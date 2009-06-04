CREATE OR REPLACE PACKAGE td_utils AUTHID CURRENT_USER
AS

   different_filesystems       EXCEPTION;
   duplicate_file              EXCEPTION;
   
   -- constants used for EXPAND_FILE
   gzip_method CONSTANT         VARCHAR2(15) := 'gzip_method';
   compress_method CONSTANT     VARCHAR2(15) := 'compress_method';
   bzip2_method CONSTANT        VARCHAR2(15) := 'bzip2_method';
   zip_method CONSTANT          VARCHAR2(15) := 'zip_method';

   -- constants used for EXPAND_FILE
   gpg_method CONSTANT          VARCHAR2(15) := 'gpg_method';

   PROCEDURE directory_list( p_directory VARCHAR2 );

   -- procedure calls Utils.runCmd java method
   FUNCTION host_cmd( p_cmd IN VARCHAR2, p_stdin IN VARCHAR2 )
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'TdUtils.hostCmd(java.lang.String, java.lang.String) return integer';

   -- procedure executes the copy_file function and translates the return code to an exception
   PROCEDURE copy_file( 
      p_source_directory VARCHAR2, 
      p_source_filename VARCHAR2, 
      p_directory VARCHAR2, 
      p_filename VARCHAR2 
   );
      
   PROCEDURE move_file( 
      p_source_directory VARCHAR2, 
      p_source_filename  VARCHAR2, 
      p_directory        VARCHAR2, 
      p_filename         VARCHAR2
   );

   -- procedure executes the delete_file function and translates the return code to an exception
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the create_file function and translates the return code to an exception
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' );

   FUNCTION get_numlines( p_directory IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER;
      
   FUNCTION get_command( p_name IN VARCHAR2 )
      RETURN VARCHAR2;

   PROCEDURE expand_file(
      p_directory   VARCHAR2, 
      p_filename    VARCHAR2,
      p_method      VARCHAR2
   );

   PROCEDURE decrypt_file( 
      p_directory      VARCHAR2, 
      p_filename       VARCHAR2,
      p_method         VARCHAR2,
      p_passphrase     VARCHAR2 DEFAULT NULL
   );

   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT NULL,
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER;

   FUNCTION extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_directory   VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT NULL,
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER;

   PROCEDURE check_table(
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL,
      p_external      VARCHAR2 DEFAULT NULL
   );
      
   PROCEDURE check_index(
      p_owner         VARCHAR2,
      p_index         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_index_type    VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL,
      p_unique        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE check_column( p_owner VARCHAR2, p_table VARCHAR2, p_column VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL );

   PROCEDURE check_object( p_owner VARCHAR2, p_object VARCHAR2, p_object_type VARCHAR2 DEFAULT NULL );

   FUNCTION get_dir_path( p_directory VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;
   
   FUNCTION index_exists( p_owner VARCHAR2, p_index VARCHAR2 )
      RETURN BOOLEAN;
      
   FUNCTION ext_table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;
      
   FUNCTION is_part_index( p_owner VARCHAR2, p_index VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION is_iot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION get_tab_part_type( p_owner VARCHAR2, p_table VARCHAR2, p_partname VARCHAR2 DEFAULT NULL )
      RETURN VARCHAR2;
      
   FUNCTION get_part_for_subpart( p_owner VARCHAR2, p_segment VARCHAR2, p_subpart VARCHAR2, p_segment_type VARCHAR2 )
      RETURN VARCHAR2;

END td_utils;
/