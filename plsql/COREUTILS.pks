CREATE OR REPLACE PACKAGE tdinc.coreutils
AS
   PROCEDURE get_dir_list (p_directory IN VARCHAR2)
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.getDirList( java.lang.String )';

   -- procedure to copy a file from one place to another
   FUNCTION copy_file (p_srcfile VARCHAR2, p_dstfile VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.copyFile( java.lang.String, java.lang.String ) return integer';

   -- procedure calls Utils.runCmd java method
   FUNCTION host_cmd (p_cmd IN VARCHAR2, p_stdin IN VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.hostCmd(java.lang.String, java.lang.String) return integer';

   -- procedure executes the copy_file function and translates the return code to an exception
   PROCEDURE copy_file (p_srcfile VARCHAR2, p_dstfile VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL);

   -- procedure executes the delete_file function and translates the return code to an exception
   PROCEDURE delete_file (p_directory VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2
            DEFAULT NULL);

   -- procedure executes the create_file function and translates the return code to an exception
   PROCEDURE create_file (p_directory VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2
            DEFAULT NULL);

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE host_cmd (p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ', p_runmode VARCHAR2
            DEFAULT NULL);

   PROCEDURE log_msg (p_msg log_table.msg%TYPE);

   PROCEDURE exec_auto (
      p_ddl           VARCHAR2,
      p_runmode       VARCHAR2 DEFAULT NULL,
      p_runmode_msg   VARCHAR2 DEFAULT 'DDL: ');

   PROCEDURE exec_sql (
      p_dml           VARCHAR2,
      p_runmode       VARCHAR2 DEFAULT NULL,
      p_runmode_msg   VARCHAR2 DEFAULT 'DML: ');

   FUNCTION get_dir_path (p_dirname VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_dir_name (p_dir_path VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_err_cd (p_name VARCHAR2)
      RETURN NUMBER;

   FUNCTION get_err_msg (p_name VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION table_exists (p_owner VARCHAR2, p_table VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION is_part_table (p_owner VARCHAR2, p_table VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION object_exists (p_owner VARCHAR2, p_object VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION is_true (p_parm VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION get_yn_ind (p_parm VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_numlines (
      p_dirname    IN   VARCHAR2,
      p_filename   IN   VARCHAR2,
      p_runmode         VARCHAR2 DEFAULT NULL)
      RETURN NUMBER;

   FUNCTION decrypt_file (
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_runmode      VARCHAR2 DEFAULT NULL)
      RETURN VARCHAR2;

   FUNCTION unzip_file (p_dirpath VARCHAR2, p_filename VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
      RETURN VARCHAR2;
END coreutils;
/