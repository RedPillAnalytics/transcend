CREATE OR REPLACE TYPE BODY mapping_ot
AS
   CONSTRUCTOR FUNCTION mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT
   AS
   BEGIN

      -- set the instrumentation details
      register( p_mapping, p_batch_id );
      
      -- load information from the mapping_conf table
      BEGIN
         SELECT manage_indexes, manage_constraints, replace_method, STATISTICS, concurrent,
                table_owner, table_name, partition_name, source_owner, source_object,
                source_column, index_regexp, index_type, partition_type, constraint_regexp,
                constraint_type
           INTO SELF.manage_indexes, SELF.manage_constraints, SELF.replace_method, SELF.STATISTICS, SELF.concurrent,
                SELF.table_owner, SELF.table_name, SELF.partition_name, SELF.source_owner, SELF.source_object,
                SELF.source_column, SELF.index_regexp, SELF.index_type, SELF.partition_type, SELF.constraint_regexp,
                SELF.constraint_type
           FROM mapping_conf
          WHERE mapping_name = SELF.mapping_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_mapping', p_mapping );
      END;

      -- confirm the properties of the mapping
      verify_map;
      -- store the batch_id
      td_inst.batch_id( p_batch_id );
      RETURN;
   END mapping_ot;

   MEMBER PROCEDURE register ( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'register' );
   BEGIN
      -- store the mapping name
      SELF.mapping_name := LOWER( p_mapping );

      -- store the batch_id
      -- only want to do this if the value is provided
      -- otherwise, keep the previous value
      IF p_batch_id IS NOT NULL
      THEN
         td_inst.batch_id( p_batch_id );
      END IF;
      -- reset the evolve_object
      o_ev.clear_app_info;
   END register;

   FINAL MEMBER PROCEDURE verify_map
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'verify_map' );
   BEGIN
      -- check to see that the specified table exists
      IF SELF.table_name IS NOT NULL
      THEN
         -- when a table_name is specified, check to make sure it exists
         td_utils.check_table( p_owner => SELF.table_owner, p_table => SELF.table_name );
      END IF;

      IF SELF.source_object IS NOT NULL
      THEN
         -- when we are doing segment switching, then the source object needs to be an actual table
         -- the target table needs to be partitioned if this is an exchange
         -- if it's not an exchange, it can be partitioned or not partitioned.
         td_utils.check_table( p_owner            => SELF.table_owner,
                               p_table            => SELF.table_name,
                               p_partitioned      => CASE replace_method
                                  WHEN 'exchange'
                                     THEN 'yes'
                                  ELSE NULL
                               END
                             );
         td_utils.check_object( p_owner            => SELF.source_owner,
                                p_object           => SELF.source_object,
                                p_object_type      => CASE
                                   WHEN replace_method IS NOT NULL
                                      THEN 'table'
                                   ELSE 'view|table'
                                END
                              );
      END IF;

      -- if exchange_method is 'rename' then a table rename is used
      -- in that case, the owner and source_owner need to be the same
      IF replace_method = 'rename' AND SELF.source_owner <> SELF.table_owner
      THEN
         evolve_log.raise_err
                          ( 'parm_not_supported',
                            'A REPLACE_METHOD value of ''exchange'' when TABLE_OWNER and SOURCE_OWNER are not the same'
                          );
      END IF;

      evolve_log.log_msg( 'Mapping confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify_map;

   MEMBER PROCEDURE start_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'etl_mapping', p_action => SELF.mapping_name );
   BEGIN
      evolve_log.log_msg( 'Starting ETL mapping' );

      -- mark indexes unusable
      IF td_core.is_true( SELF.manage_indexes ) AND SELF.replace_method IS NULL
      THEN
         td_dbutils.unusable_indexes( p_owner              => SELF.table_owner,
                                      p_table              => SELF.table_name,
                                      p_partname           => SELF.partition_name,
                                      p_source_owner       => SELF.source_owner,
                                      p_source_object      => SELF.source_object,
                                      p_source_column      => SELF.source_column,
                                      p_index_regexp       => SELF.index_regexp,
                                      p_index_type         => SELF.index_type,
                                      p_part_type          => SELF.partition_type
                                    );
      END IF;

      -- disable constraints
      IF td_core.is_true( SELF.manage_constraints ) AND SELF.replace_method IS NULL
      THEN
         td_dbutils.constraint_maint( p_owner                  => SELF.table_owner,
                                      p_table                  => SELF.table_name,
                                      p_constraint_regexp      => SELF.constraint_regexp,
                                      p_constraint_type        => SELF.constraint_type,
                                      p_maint_type             => 'disable',
                                      p_enable_queue           => 'yes'
                                    );
      END IF;
   END start_map;
   MEMBER PROCEDURE end_map
   AS
   BEGIN
      -- exchange in the partition
      CASE SELF.replace_method
         WHEN 'exchange'
         THEN
            td_dbutils.exchange_partition( p_source_owner      => SELF.source_owner,
                                           p_source_table      => SELF.source_object,
                                           p_owner             => SELF.table_owner,
                                           p_table             => SELF.table_name,
                                           p_partname          => SELF.partition_name,
                                           p_statistics        => SELF.STATISTICS,
                                           p_concurrent        => SELF.concurrent
                                         );
         -- replace the table using a rename
      WHEN 'rename'
         THEN
            td_dbutils.replace_table( p_owner             => SELF.table_owner,
                                      p_table             => SELF.table_name,
                                      p_source_table      => SELF.source_object,
                                      p_statistics        => SELF.STATISTICS,
                                      p_concurrent        => SELF.concurrent
                                    );
         ELSE
            NULL;
      END CASE;

      -- rebuild the indexes
      IF td_core.is_true( SELF.manage_indexes ) AND SELF.replace_method IS NULL
      THEN
         td_dbutils.usable_indexes( p_owner           => SELF.table_owner,
                                    p_table           => SELF.table_name,
                                    p_concurrent      => SELF.concurrent
                                  );
      END IF;

      -- enable the constraints
      IF td_core.is_true( SELF.manage_constraints ) AND SELF.replace_method IS NULL
      THEN
         td_dbutils.enable_constraints( p_concurrent => SELF.concurrent );
      END IF;
      
      -- used to be a commit right here
      -- removing it because I don't think a commit should exist inside mapping functionality
      evolve_log.log_msg( 'Ending ETL mapping' );
   END end_map;
END;
/

SHOW errors