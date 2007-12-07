CREATE OR REPLACE TYPE BODY file_ot
AS
   -- store audit information about the feed or extract
   MEMBER PROCEDURE audit_file(
      p_filepath          VARCHAR2,
      p_source_filepath   VARCHAR2,
      p_arch_filepath     VARCHAR2,
      p_num_bytes         NUMBER,
      p_num_lines         NUMBER,
      p_file_dt           DATE
   )
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'audit_file' );
   BEGIN
      o_ev.change_action( 'Insert file detail' );

      -- INSERT into the FILE_DETAIL table to record the movement
      INSERT INTO files_detail
                  ( file_detail_id, file_label, file_group,
                    file_type, source_filepath, target_filepath, arch_filepath,
                    num_bytes, num_lines, file_dt
                  )
           VALUES ( files_detail_seq.NEXTVAL, file_label, file_group,
                    file_type, p_source_filepath, p_filepath, p_arch_filepath,
                    p_num_bytes, p_num_lines, p_file_dt
                  );

      -- the job fails when size threshholds are not met
      IF NOT evolve_log.is_debugmode
      THEN

         IF p_num_bytes >= max_bytes AND max_bytes <> 0
         THEN
	    o_ev.change_action( 'file too large');
            o_ev.send( p_label => file_label );
            raise_application_error( td_inst.get_err_cd( 'file_too_large' ),
                                     td_inst.get_err_msg( 'file_too_large' )
                                   );
         ELSIF p_num_bytes < min_bytes
         THEN
	    o_ev.change_action( 'file too small');
            o_ev.send( p_label => file_label );
            raise_application_error( td_inst.get_err_cd( 'file_too_small' ),
                                     td_inst.get_err_msg( 'file_too_small' )
                                   );
         END IF;
      END IF;

      o_ev.clear_app_info;
   END audit_file;
   MEMBER PROCEDURE audit_file(
      p_num_bytes   NUMBER,
      p_num_lines   NUMBER,
      p_file_dt     DATE
   )
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'audit_file' );
   BEGIN
      o_ev.change_action( 'Insert FILE_DTL' );
      audit_file( p_filepath             => SELF.filepath,
                  p_source_filepath      => NULL,
                  p_arch_filepath        => SELF.arch_filepath,
                  p_num_bytes            => p_num_bytes,
                  p_num_lines            => p_num_lines,
                  p_file_dt              => p_file_dt
                );
   END audit_file;

   MEMBER PROCEDURE announce_file(
      p_files_url   VARCHAR2,
      p_num_lines   NUMBER,
      p_num_files   NUMBER DEFAULT 1
   )
   AS
      o_ev        evolve_ot := evolve_ot( p_module => 'announce_file' );
      l_message   notification_events.message%type;
   BEGIN
      -- notify about successful arrival of feed
      o_ev.change_action( 'Notify success' );
      l_message :=
            'The file'
         || CASE
               WHEN p_num_files > 1
                  THEN 's'
               ELSE NULL
            END
         || ' can be downloaded at the following link'
         || CASE
               WHEN p_num_files > 1
                  THEN 's'
               ELSE NULL
            END
         || ':'
         || CHR( 10 )
         || p_files_url;

      IF l_numlines > 65536
      THEN
         l_message :=
               l_message
            || CHR( 10 )
            || CHR( 10 )
            || 'The file is too large for some desktop applications, such as Microsoft Excel, to open.';
      END IF;

      o_ev.send( p_label   => self.file_label,
		 p_message => l_message );

   END announce_file;

END;
/

SHOW errors
