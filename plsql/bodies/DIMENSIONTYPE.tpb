CREATE OR REPLACE TYPE BODY dimensiontype 
as
   MEMBER PROCEDURE index_maint
   IS
      o_td               tdtype
                      := tdtype( p_module       => 'index_maint',
                                 p_runmode      => SELF.runmode );
   BEGIN
      -- type object which handles logging and application registration for instrumentation purposes
      -- defaults to registering with DBMS_APPLICATION_INFO
      o_td.change_action( 'Get count from table' );
      l_sql := 'SELECT count(*) FROM ' || SELF.object_owner || '.' || SELF.object_name;
      o_td.log_msg( 'Count SQL: ' || l_sql, 3 );

      IF NOT SELF.is_debugmode
      THEN
         BEGIN
            EXECUTE IMMEDIATE l_sql
                         INTO l_num_rows;
         EXCEPTION
            WHEN e_data_cartridge
            THEN
               -- use a regular expression to pull the KUP error out of SQLERRM
               CASE REGEXP_SUBSTR( SQLERRM, '^KUP-[[:digit:]]{5}', 1, 1, 'im' )
                  WHEN 'KUP-04040'
                  THEN
                     o_td.change_action( 'location file missing' );
                     o_td.send( p_module_id => SELF.filehub_id );
                     raise_application_error
                                            ( td_ext.get_err_cd( 'location_file_missing' ),
                                              td_ext.get_err_msg( 'location_file_missing' )
                                            );
                  ELSE
                     o_td.log_msg( 'Unknown data cartridge error' );
               END CASE;
         END;

         BEGIN
            -- calculate the percentage difference
            l_pct_miss := 100 -( ( l_num_rows / p_num_lines ) * 100 );

            IF l_pct_miss > reject_limit
            THEN
               o_td.change_action( 'reject limit exceeded' );
               -- notify if reject limit is exceeded
               o_td.send( p_module_id => SELF.filehub_id );
               raise_application_error( td_ext.get_err_cd( 'reject_limit_exceeded' ),
                                        td_ext.get_err_msg( 'reject_limit_exceeded' )
                                      );
            END IF;
         EXCEPTION
            WHEN ZERO_DIVIDE
            THEN
               o_td.log_msg( 'External table location is an empty file' );
         END;

         INSERT INTO filehub_obj_detail
                     ( filehub_obj_id, filehub_id,
                       filehub_type, filehub_name, filehub_group,
                       object_owner, object_name, num_rows, num_lines,
                       percent_diff
                     )
              VALUES ( filehub_obj_detail_seq.NEXTVAL, SELF.filehub_id,
                       SELF.filehub_type, SELF.filehub_name, SELF.filehub_group,
                       SELF.object_owner, SELF.object_name, l_num_rows, p_num_lines,
                       l_pct_miss
                     );
      END IF;

      o_td.clear_app_info;
   EXCEPTION
      WHEN e_no_table
      THEN
         raise_application_error( td_ext.get_err_cd( 'no_tab' ),
                                     td_ext.get_err_msg( 'no_tab' )
                                  || ': '
                                  || SELF.object_owner
                                  || '.'
                                  || SELF.object_name
                                );
   END index_maint;
/