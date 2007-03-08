CREATE OR REPLACE PACKAGE tdinc.coreutils
AS
   PROCEDURE get_dir_list (p_directory IN VARCHAR2)
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.getDirList( java.lang.String )';
      
   -- procedure to copy a file from one place to another
      FUNCTION copy_file (p_srcfile VARCHAR2, p_dstfile varchar2)
	 RETURN NUMBER
   AS
      LANGUAGE JAVA
	 NAME 'CoreUtils.copyFile( java.lang.String, java.lang.String ) return integer';

   -- procedure to copy a file from one place to another
   FUNCTION delete_file (p_srcfile VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.deleteFile( java.lang.String ) return integer';      

   -- procedure calls Utils.runCmd java method
   FUNCTION host_cmd (p_cmd IN VARCHAR2, p_stdin IN VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'CoreUtils.hostCmd(java.lang.String, java.lang.String) return integer';

   -- procedure executes the copy_file function and translates the return code to an exception      
      PROCEDURE copy_file (p_srcfile VARCHAR2, p_dstfile varchar2, p_debug BOOLEAN DEFAULT FALSE);
      
   -- procedure executes the delete_file function and translates the return code to an exception      
   PROCEDURE delete_file (p_srcfile VARCHAR2, p_debug BOOLEAN DEFAULT FALSE);

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
      p_notification_id     notification.notification_id%TYPE,
      p_component_id        NUMBER,
      p_detail_id           NUMBER,
      p_notification_type   notification.notification_type%TYPE DEFAULT NULL,
      p_sender              notification.sender%TYPE DEFAULT NULL,
      p_recipients          notification.recipients%TYPE DEFAULT NULL,
      p_subject             notification.subject%TYPE DEFAULT NULL,
      p_message             notification.MESSAGE%TYPE DEFAULT NULL,
      p_baseurl             notification.baseurl%TYPE DEFAULT NULL,
      p_debug               BOOLEAN DEFAULT FALSE);

   FUNCTION unzip_file (p_dirpath VARCHAR2, p_filename VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
      RETURN VARCHAR2;
END coreutils;
/