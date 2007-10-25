CREATE OR REPLACE PACKAGE td_core AUTHID CURRENT_USER
AS
   TYPE t_split IS TABLE OF VARCHAR2(32767);

   PROCEDURE get_dir_list( p_directory IN VARCHAR2 )
   AS
      LANGUAGE JAVA
      NAME 'TdCore.getDirList( java.lang.String )';

   -- procedure to copy a file from one place to another
   FUNCTION copy_file( p_srcfile VARCHAR2, p_dstfile VARCHAR2 )
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'TdCore.copyFile( java.lang.String, java.lang.String ) return integer';

   -- procedure calls Utils.runCmd java method
   FUNCTION host_cmd( p_cmd IN VARCHAR2, p_stdin IN VARCHAR2 )
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'TdCore.hostCmd(java.lang.String, java.lang.String) return integer';

   -- procedure executes the copy_file function and translates the return code to an exception
   PROCEDURE copy_file( p_srcfile VARCHAR2, p_dstfile VARCHAR2 );

   -- procedure executes the delete_file function and translates the return code to an exception
   PROCEDURE delete_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the create_file function and translates the return code to an exception
   PROCEDURE create_file( p_directory VARCHAR2, p_filename VARCHAR2 );

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE host_cmd( p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ' );

   FUNCTION get_numlines( p_dirname IN VARCHAR2, p_filename IN VARCHAR2 )
      RETURN NUMBER;

   FUNCTION decrypt_file( p_dirpath VARCHAR2, p_filename VARCHAR2, p_passphrase VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION unzip_file( p_dirpath VARCHAR2, p_filename VARCHAR2 )
      RETURN VARCHAR2;

END td_core;
/