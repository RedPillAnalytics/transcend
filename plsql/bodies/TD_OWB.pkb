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
      p_batch_id        NUMBER DEFAULT NULL
   )
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'start_map_control' );
   BEGIN
      o_ev.change_action( REGEXP_SUBSTR( td_inst.whence, '\S+$', 1, 1, 'i' ));
      td_inst.batch_id( p_batch_id );
      td_inst.log_msg( 'Beginning OWB mapping' );

      -- see whether or not to call UNUSABLE_INDEXES
      IF p_owner IS NOT NULL AND p_table IS NOT NULL
      THEN
         td_ddl.unusable_indexes( p_owner              => p_owner,
                                  p_table              => p_table,
                                  p_partname           => p_partname,
                                  p_source_owner       => p_source_owner,
                                  p_source_object      => p_source_object,
                                  p_source_column      => p_source_column,
                                  p_d_num              => NVL( p_d_num, 0 ),
                                  p_p_num              => NVL( p_p_num, 65535 ),
                                  p_index_regexp       => p_index_regexp,
                                  p_index_type         => p_index_type,
                                  p_part_type          => p_part_type
                                );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         td_inst.log_err;
         RAISE;
   END start_map_control;

   PROCEDURE end_map_control(
      p_owner          VARCHAR2 DEFAULT NULL,
      p_table          VARCHAR2 DEFAULT NULL,
      p_source_owner   VARCHAR2 DEFAULT NULL,
      p_source_table   VARCHAR2 DEFAULT NULL,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_space    VARCHAR2 DEFAULT NULL,
      p_index_drop     VARCHAR2 DEFAULT NULL,
      p_statistics     VARCHAR2 DEFAULT NULL
   )
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'end_map_control' );
   BEGIN
      o_ev.change_action( REGEXP_SUBSTR( td_inst.whence, '\S+$', 1, 1, 'i' ));

      CASE
         WHEN p_source_owner IS NOT NULL AND p_source_table IS NOT NULL
         THEN
            td_ddl.exchange_partition( p_source_owner      => p_source_owner,
                                       p_source_table      => p_source_table,
                                       p_owner             => p_owner,
                                       p_table             => p_table,
                                       p_partname          => p_partname,
                                       p_index_space       => p_index_space,
                                       p_index_drop        => NVL( p_index_drop, 'yes' ),
                                       p_statistics        => p_statistics
                                     );
         WHEN p_owner IS NOT NULL AND p_table IS NOT NULL
         THEN
            td_ddl.usable_indexes( p_owner, p_table );
         ELSE
            NULL;
      END CASE;

      td_inst.log_msg( 'Ending OWB mapping' );
      o_ev.clear_app_info;
   END end_map_control;
END td_owbapi;
/