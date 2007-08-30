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
END td_owbapi;
/