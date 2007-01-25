CREATE OR REPLACE PACKAGE efw.file_adm
IS
   PROCEDURE process_job (
      p_jobname       VARCHAR2,
      p_filename      VARCHAR2 DEFAULT NULL,
      p_keep_source   BOOLEAN DEFAULT FALSE,
      p_debug         BOOLEAN DEFAULT FALSE);

END file_adm;
/