CREATE OR REPLACE PACKAGE tdinc.core_utils
AS
   PROCEDURE get_dir_list (p_directory IN VARCHAR2)
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.getDirList( java.lang.String )';

   -- procedure calls Utils.runCmd java method
   FUNCTION host_cmd (p_cmd IN VARCHAR2, p_stdin IN VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.hostCmd(java.lang.String, java.lang.String) return integer';

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE host_cmd (p_cmd VARCHAR2, p_stdin VARCHAR2 DEFAULT ' ', p_debug BOOLEAN DEFAULT FALSE);

   PROCEDURE log_msg (p_msg log_table.msg%TYPE);

   PROCEDURE ddl_exec (
      p_ddl         VARCHAR2,
      p_debug_msg   VARCHAR2 DEFAULT 'DDL statememt: ',
      p_debug       BOOLEAN DEFAULT FALSE);

   FUNCTION get_dir_path (p_dirname VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_dir_name (p_dir_path VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_numlines (p_dirname IN VARCHAR2, p_filename IN VARCHAR2)
      RETURN NUMBER;

   PROCEDURE notify (
      p_notification_id   notification.notification_id%TYPE,
      p_module            notification.module%TYPE,
      p_module_id         notification.module_id%TYPE,
      p_debug             BOOLEAN DEFAULT FALSE);

   FUNCTION unzip_file (p_dirpath VARCHAR2, p_filename VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
      RETURN VARCHAR2;
END core_utils;
/