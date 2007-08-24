CREATE OR REPLACE PACKAGE BODY td_owbapi
AS
   PROCEDURE start_map_control(
      p_owner           VARCHAR2 DEFAULT NULL,
      p_table           VARCHAR2 DEFAULT NULL,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT NULL,
      p_p_num           NUMBER DEFAULT NULL,
      p_index_regexp    VARCHAR2 DEFAULT NULL,
      p_index_type      VARCHAR2 DEFAULT NULL,
      p_part_type       VARCHAR2 DEFAULT NULL,
      p_oper_id         NUMBER DEFAULT NULL,
      p_runmode         VARCHAR2 DEFAULT NULL
   )
   AS
      o_td   tdtype := tdtype( p_module => 'start_map_control', p_runmode => p_runmode );
   BEGIN
      o_td.change_action( REGEXP_SUBSTR( o_td.whence, '\S+$', 1, 1, 'i' ));
      o_td.log_msg( 'Beginning OWB mapping', p_oper_id => p_oper_id );

      -- see whether or not to call UNUSABLE_INDEXES
      IF p_owner IS NOT NULL AND p_table IS NOT NULL
      THEN
         td_dbapi.unusable_indexes( p_owner              => p_owner,
                                    p_table              => p_table,
                                    p_partname           => p_partname,
                                    p_source_owner       => p_source_owner,
                                    p_source_object      => p_source_object,
                                    p_source_column      => p_source_column,
                                    p_d_num              => NVL( p_d_num, 0 ),
                                    p_p_num              => NVL( p_p_num, 65535 ),
                                    p_index_regexp       => p_index_regexp,
                                    p_index_type         => p_index_type,
                                    p_part_type          => p_part_type,
                                    p_runmode            => o_td.runmode
                                  );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_td.log_err;
         RAISE;
   END start_map_control;

   PROCEDURE end_map_control(
      p_owner            VARCHAR2 DEFAULT NULL,
      p_table            VARCHAR2 DEFAULT NULL,
      p_source_owner     VARCHAR2 DEFAULT NULL,
      p_source_table     VARCHAR2 DEFAULT NULL,
      p_partname         VARCHAR2 DEFAULT NULL,
      p_idx_tablespace   VARCHAR2 DEFAULT NULL,
      p_index_drop       VARCHAR2 DEFAULT NULL,
      p_handle_fkeys     VARCHAR2 DEFAULT NULL,
      p_statistics       VARCHAR2 DEFAULT NULL,
      p_oper_id          NUMBER DEFAULT NULL,
      p_runmode          VARCHAR2 DEFAULT NULL
   )
   AS
      o_td   tdtype := tdtype( p_module => 'end_map_control', p_runmode => p_runmode );
   BEGIN
      o_td.change_action( REGEXP_SUBSTR( o_td.whence, '\S+$', 1, 1, 'i' ));

      CASE
         WHEN p_source_owner IS NOT NULL AND p_source_table IS NOT NULL
         THEN
            td_dbapi.exchange_partition( p_source_owner        => p_source_owner,
                                         p_source_table        => p_source_table,
                                         p_owner               => p_owner,
                                         p_table               => p_table,
                                         p_partname            => p_partname,
                                         p_idx_tablespace      => p_idx_tablespace,
                                         p_index_drop          => NVL( p_index_drop,
                                                                       'yes' ),
                                         p_handle_fkeys        => NVL( p_handle_fkeys,
                                                                       'yes'
                                                                     ),
                                         p_statistics          => p_statistics,
                                         p_runmode             => o_td.runmode
                                       );
         WHEN p_owner IS NOT NULL AND p_table IS NOT NULL
         THEN
            td_dbapi.usable_indexes( p_owner, p_table, p_runmode => o_td.runmode );
         ELSE
            NULL;
      END CASE;

      o_td.log_msg( 'Ending OWB mapping', p_oper_id => p_oper_id );
      o_td.clear_app_info;
   END end_map_control;
   
   PROCEDURE run_process_flow(
      p_flow_name       VARCHAR2,
      p_flow_location   VARCHAR2,
      p_rep_owner	VARCHAR2,
      p_runmode         VARCHAR2 DEFAULT NULL
   )
   AS
      o_td   tdtype := tdtype( p_module => 'run_process_flow', p_runmode => p_runmode );
      l_retval   NUMBER;
      l_results  NUMBER;
      l_sql LONG;
      l_cur_schema VARCHAR2(30) := sys_context('USERENV','CURRENT_SCHEMA');
   BEGIN
      l_sql := 'BEGIN :retval := wb_rt_api_exec.run_task(upper(:flow_location),''PROCESSFLOW'',UPPER(:flow_name),null,null,1); END;';
      l_results := td_sql.exec_sql('alter session set current_schema='||p_rep_owner,p_runmode=>p_runmode);
      IF o_td.is_debugmode
      THEN
	 o_td.log_msg('SQL: '||l_sql);
      ELSE
	 EXECUTE IMMEDIATE
	 l_sql
	 USING OUT l_retval, IN p_flow_location, IN p_flow_name;
	 IF l_retval <> 0
	 THEN
	    raise_application_error( td_ext.get_err_cd('owb_flow_err'),
				     td_ext.get_err_msg('owb_flow_err'));
	 END IF;
      END IF;
      l_results := td_sql.exec_sql('alter session set current_schema='||l_cur_schema,p_runmode=>p_runmode);
      o_td.clear_app_info;
   EXCEPTION
   WHEN others
   THEN
      l_results := td_sql.exec_sql('alter session set current_schema='||l_cur_schema,p_runmode=>p_runmode);
      o_td.log_err;
      RAISE;
   END run_process_flow;

END td_owbapi;
/