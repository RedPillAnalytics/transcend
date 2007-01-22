CREATE OR REPLACE PACKAGE efw.util
AS
   PROCEDURE get_dir_list (p_directory IN VARCHAR2)
   AS
      LANGUAGE JAVA
      NAME 'Util.getDirList( java.lang.String )';

   -- procedure calls Utils.runCmd java method
   FUNCTION run_cmd (p_cmd IN VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'Util.runCmd(java.lang.String) return integer';

   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE run_cmd (p_cmd IN VARCHAR2);

   -- Function calls Utils.decryptFile java method
   -- returns a return code that tries to decipher the success of the java code
   FUNCTION gpg_decrypt_file (p_srcfile VARCHAR2, p_trgfile VARCHAR2, p_passphrase VARCHAR2)
      RETURN NUMBER
   AS
      LANGUAGE JAVA
      NAME 'Util.pgpDecryptFile(java.lang.String, java.lang.String, java.lang.String) return integer';

   -- procedure calls Utils.decryptFile java method
   PROCEDURE gpg_decrypt_file (p_srcfile VARCHAR2, p_trgfile VARCHAR2, p_passphrase VARCHAR2);
END util;
/