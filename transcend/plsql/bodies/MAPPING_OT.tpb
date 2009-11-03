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
      REGISTER( p_mapping, p_batch_id );

      -- load information from the mapping_conf table
      BEGIN
         SELECT manage_indexes, manage_constraints, replace_method, STATISTICS, index_concurrency, constraint_concurrency,
                table_owner, table_name, partition_name, source_owner, source_object,
                source_column, index_regexp, index_type, partition_type, constraint_regexp,
                constraint_type, mapping_type
           INTO SELF.manage_indexes, SELF.manage_constraints, SELF.replace_method, SELF.STATISTICS, SELF.index_concurrency, SELF.constraint_concurrency,
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
      SELF.mapping_name    := LOWER( regexp_replace(p_mapping,'^"|"$',NULL));

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
      l_tab_part       BOOLEAN;

      o_ev   evolve_ot := evolve_ot( p_module => 'verify' );
   BEGIN      

      -- if the table name is not null
      -- then that means the mapping is associated with a table
      -- and we will most likely have things to do on that table
      IF SELF.table_name IS NOT NULL
      THEN

         -- make sure we also have a table owner
         IF self.table_owner IS NULL
         THEN
            evolve.raise_err( 'parms_group', 'P_OWNER and P_TABLE' );
         END IF;
         
         -- when a table_name is specified, check to make sure it exists
         td_utils.check_table( p_owner => SELF.table_owner, p_table => SELF.table_name );
         
         -- let's find out if it's partitioned
         l_tab_part      := td_utils.is_part_table( table_owner, table_name);

      END IF;
      
      -- if source object is not null, then there are two possibilites
      -- we are either affecting indexes on TABLE based on this source
      -- or, we are doing a table replace of some sorts
      -- this is determined by REPLACE_METHOD... if it's null, then only indexes are affected
      IF SELF.source_object IS NOT NULL
      THEN
         
         -- make sure we also have a regular table in place
         IF self.table_owner IS NULL
         THEN
            evolve.raise_err( 'parms_group', 'P_SOURCE_OBJECT and P_TABLE' );
         END IF;
         
         -- make sure the table name has an owner
         IF self.source_owner IS NULL
         THEN
            evolve.raise_err( 'parms_group', 'P_SOURCE_OWNER and P_SOURCE_OBJECT' );
         END IF;
         
         -- when a source object is specified, check to make sure it exists
         td_utils.check_object( p_owner            => SELF.source_owner,
                                p_object           => SELF.source_object,
                                p_object_type      => 'table|view'
                              );
         
         -- now, depending on REPLACE_METHOD, we are either affecting indexes only or doing a replace
         IF replace_method = 'exchange' AND mapping_type = 'table'
         THEN
   
            -- we need to find out whether source object is partitioned or not         
            l_src_part      := td_utils.is_part_table( source_owner, source_object);
   
            IF l_src_part AND l_tab_part
            THEN
               evolve.raise_err( 'both_part' );
            ELSIF (NOT l_src_part AND NOT l_tab_part)
            THEN
               evolve.raise_err( 'neither_part' );
            END IF;
            
         END IF;
         
      ELSE
         
         -- source object is null, but we want to exchange or rename
         -- that's not cool
         IF replace_method IS NOT NULL
         THEN
            evolve.raise_err('no_source_tab');
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

      -- we want to manage indexes, but there is no table replace that is occurring
      -- so we need to explicitly manage the indexes
      -- so we need to mark indexes unusable beforehand
      IF lower(self.manage_indexes) IN ('unusable','both') AND self.replace_method IS NULL
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

      -- we want to manage constraints, but there is no table replace that is occurring
      -- so we need to explicitly manage constraints
      -- so we need to disable constraints beforehand
      IF lower(self.manage_constraints) IN ('disable','both') AND self.replace_method IS NULL
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

      -- we exchange the partition
      -- this handles constraints and indexes
      CASE SELF.replace_method
         WHEN 'exchange'
         THEN
            td_dbutils.exchange_partition( p_source_owner      => SELF.source_owner,
                                           p_source_table      => SELF.source_object,
                                           p_owner             => SELF.table_owner,
                                           p_table             => SELF.table_name,
                                           p_partname          => SELF.partition_name,
                                           p_statistics        => SELF.STATISTICS,
                                           p_idx_concurrency   => SELF.index_concurrency,
                                           p_con_concurrency   => SELF.constraint_concurrency
                                         );
         -- replace the table using a rename
         -- this handles constraints and indexes
         WHEN 'rename'
         THEN
            td_dbutils.replace_table( p_owner             => SELF.table_owner,
                                      p_table             => SELF.table_name,
                                      p_source_table      => SELF.source_object,
                                      p_statistics        => SELF.STATISTICS,
                                      p_idx_concurrency   => SELF.index_concurrency,
                                      p_con_concurrency   => SELF.constraint_concurrency
                                    );
         ELSE
           NULL;
      END CASE;
      
      -- if there is no replace method, then we need to rebuild indexes
      -- rebuild the indexes
      IF lower(self.manage_indexes) IN ('usable','both') AND self.replace_method IS NULL
      THEN
         td_dbutils.usable_indexes( p_owner           => SELF.table_owner,
                                    p_table           => SELF.table_name,
                                    p_concurrent      => SELF.index_concurrency
                                  );
      END IF;

      -- enable the constraints
      -- if there is no replace method, then we need to enable constraints
      IF lower(self.manage_constraints) IN ('enable','both') AND self.replace_method IS NULL
      THEN
         td_dbutils.constraint_maint( p_owner                  => SELF.table_owner,
                                      p_table                  => SELF.table_name,
                                      p_constraint_regexp      => SELF.constraint_regexp,
                                      p_constraint_type        => SELF.constraint_type,
                                      p_maint_type             => 'enable',
                                      p_concurrent	       => SELF.constraint_concurrency
                                    );

      END IF;

      -- this is not a segment-switching situation
      -- there is a table name specified
      -- 'gather' is specified for statistics
      IF self.replace_method IS NULL
         AND self.table_name IS NOT NULL 
         AND REGEXP_LIKE( 'gather', self.statistics, 'i' )
      THEN
         td_dbutils.gather_stats( p_owner               => self.table_owner, 
                                  p_segment             => self.table_name, 
                                  p_segment_type        => 'table' );
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