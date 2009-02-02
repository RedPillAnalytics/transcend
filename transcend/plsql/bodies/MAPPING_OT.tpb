CREATE OR REPLACE TYPE BODY mapping_ot
AS
   CONSTRUCTOR FUNCTION mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT
   AS
   BEGIN
      -- there is an owb constant that can be used to get the mapping name
      -- however, the constant puts double quotes around it
      -- need to strip these double quotes just in case
      -- set the instrumentation details
      REGISTER( regexp_replace(p_mapping,'^"|"$',NULL), p_batch_id );

      -- load information from the mapping_conf table
      BEGIN
         SELECT manage_indexes, manage_constraints, replace_method, STATISTICS, concurrent,
                table_owner, table_name, partition_name, source_owner, source_object,
                source_column, index_regexp, index_type, partition_type, constraint_regexp,
                constraint_type, mapping_type
           INTO SELF.manage_indexes, SELF.manage_constraints, SELF.replace_method, SELF.STATISTICS, SELF.concurrent,
                SELF.table_owner, SELF.table_name, SELF.partition_name, SELF.source_owner, SELF.source_object,
                SELF.source_column, SELF.index_regexp, SELF.index_type, SELF.partition_type, SELF.constraint_regexp,
                SELF.constraint_type, SELF.mapping_type
           FROM mapping_conf
          WHERE mapping_name = SELF.mapping_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_mapping', p_mapping );
      END;

      -- confirm the properties of the mapping
      verify;

      -- store the batch_id
      td_inst.batch_id( p_batch_id );
      RETURN;
   END mapping_ot;
   MEMBER PROCEDURE REGISTER( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'register' );
   BEGIN
      -- store the mapping name
      SELF.mapping_name    := LOWER( p_mapping );

      -- store the batch_id
      -- only want to do this if the value is provided
      -- otherwise, keep the previous value
      IF p_batch_id IS NOT NULL
      THEN
         td_inst.batch_id( p_batch_id );
      END IF;

      -- reset the evolve_object
      o_ev.clear_app_info;
   END REGISTER;
   MEMBER PROCEDURE verify
   IS
      l_src_part       BOOLEAN;
      l_trg_part       BOOLEAN;
      l_src_part_flg   VARCHAR2(3);
      l_trg_part_flg   VARCHAR2(3);

      o_ev   evolve_ot := evolve_ot( p_module => 'verify' );
   BEGIN
      -- check to see that the specified table exists
      IF SELF.table_name IS NOT NULL
      THEN
         -- when a table_name is specified, check to make sure it exists
         td_utils.check_table( p_owner => SELF.table_owner, p_table => SELF.table_name );
      END IF;

      IF SELF.source_object IS NOT NULL
      THEN
         
         IF replace_method = 'exchange' AND mapping_type = 'table'
         THEN

            -- if we are doing segment switching, then one of the tables needs to be partitioned
            -- but they both can't be
            
            o_ev.change_action( 'determine partitioned table');
            -- find out which tables are partitioned
            l_src_part      := td_utils.is_part_table( source_owner, source_object);
            l_src_part_flg  := CASE WHEN l_src_part THEN 'yes' ELSE 'no' END;
            evolve.log_msg( 'Variable L_SRC_PART_FLG: '||l_src_part_flg, 5 );
            l_trg_part      := td_utils.is_part_table( table_owner, table_name );
            l_trg_part_flg  := CASE WHEN l_trg_part THEN 'yes' ELSE 'no' END;
            evolve.log_msg( 'Variable L_TRG_PART_FLG: '||l_trg_part_flg, 5 );
            
            CASE
              -- raise exceptions if both are partitioned
              WHEN l_src_part AND l_trg_part
              THEN
                 evolve.raise_err( 'both_part' );
              -- raise exceptions if neither are partitioned
              WHEN NOT l_src_part AND NOT l_trg_part
              THEN
                 evolve.raise_err( 'neither_part' );
              ELSE
                 NULL;
            END CASE;
               
         END IF;

      END IF;

      -- if exchange_method is 'rename' then a table rename is used
      -- in that case, the owner and source_owner need to be the same
      IF SELF.replace_method = 'rename' AND SELF.source_owner <> SELF.table_owner AND SELF.mapping_type = 'table'
      THEN
         evolve.raise_err
            ( 'parm_not_supported',
              'A REPLACE_METHOD value of ''exchange'' when MAPPING_TYPE is ''table'' and TABLE_OWNER and SOURCE_OWNER are not the same'
            );
      END IF;

      evolve.log_msg( 'Mapping confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;
   MEMBER PROCEDURE start_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping '||SELF.mapping_name, p_action => 'start mapping' );
   BEGIN
      evolve.log_msg( 'Pre-mapping processes beginning' );

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
                                      p_maint_type             => 'disable'
                                    );
      END IF;
      evolve.log_msg( 'Pre-mapping processes completed' );
      o_ev.change_action( 'execute mapping' );
   END start_map;
   MEMBER PROCEDURE end_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping '||SELF.mapping_name, p_action => 'end mapping' );
   BEGIN
      evolve.log_msg( 'Post-mapping processes beginning' );

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
         td_dbutils.constraint_maint( p_owner                  => SELF.table_owner,
                                      p_table                  => SELF.table_name,
                                      p_constraint_regexp      => SELF.constraint_regexp,
                                      p_constraint_type        => SELF.constraint_type,
                                      p_maint_type             => 'enable',
                                      p_concurrent	       => SELF.concurrent
                                    );

      END IF;

      -- used to be a commit right here
      -- removing it because I don't think a commit should exist inside mapping functionality
      evolve.log_msg( 'Post-mapping processes completed' );
      o_ev.clear_app_info;
   END end_map;
   -- null procedure for polymorphism only
   MEMBER PROCEDURE LOAD
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'load' );
   BEGIN
      -- simply raise an exception if this procedure ever gets called
      -- it never should, as it is only here for inheritance
      evolve.raise_err( 'wrong_map_type' );
      o_ev.clear_app_info;
   END LOAD;
   -- null procedure for polymorphism only
   MEMBER PROCEDURE confirm_dim_cols
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'confirm_dim_cols' );
   BEGIN
      -- simply raise an exception if this procedure ever gets called
      -- it never should, as it is only here for inheritance
      evolve.raise_err( 'wrong_map_type' );
      o_ev.clear_app_info;
   END confirm_dim_cols;
END;
/

SHOW errors