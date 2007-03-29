CREATE OR REPLACE TYPE BODY tdinc.fhconf
AS
   -- store audit information about the feed or extract
   MEMBER PROCEDURE audit_file (
      p_filepath          VARCHAR2,
      p_source_filepath   VARCHAR2,
      p_arch_filepath     VARCHAR2,
      p_num_bytes         NUMBER,
      p_num_lines         NUMBER,
      p_file_dt           DATE,
      p_validate          BOOLEAN DEFAULT TRUE)
   AS
      o_app   applog := applog (p_module => 'fhconf.audit_file', p_debug => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Insert FILEHUB_DETAIL');

      -- INSERT into the FILE_DETAIL table to record the movement
      INSERT INTO filehub_detail
                  (fh_detail_id,
                   filehub_id,
                   filehub_name,
                   filehub_group,
                   filehub_type,
                   source_filepath,
                   target_filepath,
                   arch_filepath,
                   num_bytes,
                   num_lines,
                   file_dt)
           VALUES (filehub_detail_seq.NEXTVAL,
                   filehub_id,
                   filehub_name,
                   filehub_group,
                   filehub_type,
                   p_source_filepath,
                   p_filepath,
                   p_arch_filepath,
                   p_num_bytes,
                   p_num_lines,
                   p_file_dt);

      -- the job fails when size threshholds are not met
      o_app.set_action ('Check file details');

      IF NOT SELF.DEBUG_MODE AND p_validate
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
   MEMBER PROCEDURE audit_file (
      p_num_bytes   NUMBER,
      p_num_lines   NUMBER,
      p_file_dt     DATE,
      p_validate    BOOLEAN DEFAULT TRUE)
   AS
      o_app   applog := applog (p_module => 'fhconf.audit_file', p_debug => SELF.DEBUG_MODE);
   BEGIN
      o_app.set_action ('Insert FILE_DTL');
      audit_file (p_filepath             => SELF.filepath,
                  p_source_filepath      => NULL,
                  p_arch_filepath        => SELF.arch_filepath,
                  p_num_bytes            => p_num_bytes,
                  p_num_lines            => p_num_lines,
                  p_file_dt              => p_file_dt);
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END audit_file;
   MEMBER PROCEDURE send (p_action VARCHAR2, p_module VARCHAR2, p_message VARCHAR2 DEFAULT NULL)
   AS
      o_notify   notify;
      o_app      applog := applog (p_module => 'fhconf.send', p_debug => SELF.DEBUG_MODE);
   BEGIN

      IF self.debug_mode
      THEN
	 o_app.log_msg('The notification action is: '||p_action);
	 o_app.log_msg('The notification module is: '||p_module);
      END IF;

      SELECT VALUE (t)
        INTO o_notify
        FROM notify_ot t
       WHERE t.module_id = SELF.filehub_id
         AND LOWER (t.action) = LOWER (p_action)
         AND LOWER (t.module) = LOWER (p_module);

      o_notify.DEBUG_MODE (SELF.DEBUG_MODE);
      o_notify.MESSAGE :=
         CASE p_message
            WHEN NULL
               THEN o_notify.MESSAGE
            ELSE o_notify.MESSAGE || CHR (10) || CHR (10) || p_message
         END;
      o_notify.module := p_module;
      o_notify.action := p_action;
      o_notify.send;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_app.log_msg ('Notification not configured for this action');
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END send;
END;
/