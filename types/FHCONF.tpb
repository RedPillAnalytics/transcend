CREATE OR REPLACE TYPE BODY tdinc.fhconf
AS
   -- store audit information about the feed or extract
   MEMBER PROCEDURE audit_file (
      p_src_filename   VARCHAR2 DEFAULT NULL,
      p_num_bytes      NUMBER,
      p_num_lines      NUMBER,
      p_file_dt        DATE
   )
   AS
      o_app   applog
         := applog (p_module => 'FILE_MOVER.AUDIT_FILE',
                    p_debug => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Insert FILE_DTL');

      -- INSERT into the FILE_DETAIL table to record the movement
      INSERT INTO filehub_detail
                  (fh_detail_id, filehub_id, filehub_name,
                   filehub_group, filehub_type, src_filename, trg_filename,
                   arch_filename, num_bytes, num_lines, file_dt
                  )
           VALUES (filehub_detail_seq.NEXTVAL, filehub_id, filehub_name,
                   filehub_group, filehub_type, p_src_filename, filepath,
                   arch_filepath, p_num_bytes, p_num_lines, p_file_dt
                  );

      -- the job fails when size threshholds are not met
      o_app.set_action ('Check file details');

      IF NOT SELF.DEBUG_MODE
      THEN
         IF p_num_bytes >= max_bytes AND max_bytes <> 0
         THEN
            raise_application_error (o_app.get_err_cd ('file_too_large'),
                                     o_app.get_err_msg ('file_too_large'));
         ELSIF p_num_bytes < min_bytes
         THEN
            raise_application_error (o_app.get_err_cd ('file_too_small'),
                                     o_app.get_err_msg ('file_too_small'));
         END IF;
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END audit_file;
END;
/