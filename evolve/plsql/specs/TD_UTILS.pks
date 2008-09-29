CREATE OR REPLACE PACKAGE td_utils AUTHID CURRENT_USER
AS

   different_filesystems       EXCEPTION;
   duplicate_file              EXCEPTION;
   
   -- constants used for EXPAND_FILE
   CONSTANT gzip_method         VARCHAR2(15) := 'gzip_method';
   CONSTANT compress_method     VARCHAR2(15) := 'compress_method';
   CONSTANT bzip_method         VARCHAR2(15) := 'bzip_method';
   CONSTANT zip_method          VARCHAR2(15) := 'zip_method';

   -- constants used for EXPAND_FILE
   CONSTANT gpg_method          VARCHAR2(15) := 'gpg_method';

   -- constant for both EXPAND_FILE and DECRYPT_FILE
   -- this constant is used to dictate that a file extension should determin the method used
   CONSTANT extension_method    VARCHAR2(15) := 'extension_based';

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
      p_filename VARCHAR2 );

   -- procedure executes the delete_file function and translates the return code to an exception
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the create_file function and translates the return code to an exception
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' );

   FUNCTION get_numlines( p_directory IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER;

   PROCEDURE expand_file( 
      p_directory   VARCHAR2, 
      p_filename    VARCHAR2,
      r_filename    VARCHAR2 OUT,
      r_filesize    NUMBER   OUT,
      r_blocksize   NUMBER   OUT,
      r_expanded    NUMBER   OUT,
      p_comp_method DEFAULT extension_method
   );

   PROCEDURE decrypt_file( 
      p_directory      VARCHAR2, 
      p_filename       VARCHAR2,
      p_passphrase     VARCHAR2,
      r_filename       VARCHAR2 OUT,
      r_filesize       NUMBER   OUT,
      r_blocksize      NUMBER   OUT,
      r_decrypted      BOOLEAN  OUT,
      p_encrypt_method DEFAULT extension_method
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

   PROCEDURE check_column( p_owner VARCHAR2, p_table VARCHAR2, p_column VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL );

   PROCEDURE check_object( p_owner VARCHAR2, p_object VARCHAR2, p_object_type VARCHAR2 DEFAULT NULL );

   FUNCTION get_dir_path( p_directory VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION ext_table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION is_iot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN;
END td_utils;
/