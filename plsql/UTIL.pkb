CREATE OR REPLACE PACKAGE BODY common.util
AS
   -- procedure executes the run_cmd function and raises an exception with the return code
   PROCEDURE run_cmd (
      p_cmd   IN   VARCHAR2)
   AS
      l_retval   NUMBER;
      l_app      app_info := app_info (p_module      => 'UTIL.RUM_CMD');
   BEGIN
      l_retval := run_cmd (p_cmd);

      IF l_retval <> 0
      THEN
         raise_application_error (-20020,
                                  'Java Error: method Util.rumCmd made unsuccessful system calls');
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END run_cmd;

   PROCEDURE gpg_decrypt_file (
      p_srcfile      VARCHAR2,
      p_trgfile      VARCHAR2,
      p_passphrase   VARCHAR2)
   AS
      l_retval   NUMBER;
      l_app      app_info := app_info (p_module      => 'UTIL.GPG_DECRYPT_FILE');
   BEGIN
      l_retval := gpg_decrypt_file (p_srcfile,
                                    p_trgfile,
                                    p_passphrase);

      IF l_retval <> 0
      THEN
         raise_application_error
                           (-20020,
                            'Java Error: method Util.pgpDecryptFile made unsuccessful system calls');
      END IF;

      l_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         job.log_err;
         RAISE;
   END gpg_decrypt_file;
END util;
/