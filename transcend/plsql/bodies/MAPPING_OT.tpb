CREATE OR REPLACE TYPE BODY mapping_ot
AS
   CONSTRUCTOR FUNCTION mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT
   AS
   BEGIN
      -- set the instrumentation details
      REGISTER( p_mapping, p_batch_id );

      -- load information from the mapping_conf table
      BEGIN
         SELECT manage_indexes, manage_constraints, replace_method, STATISTICS, index_concurrency, constraint_concurrency,
                table_owner, table_name, partition_name, staging_owner, staging_table,
                staging_column, index_regexp, index_type, partition_type, constraint_regexp,
                constraint_type, mapping_type, drop_dependent_objects, restartable
           INTO SELF.manage_indexes, SELF.manage_constraints, SELF.replace_method, SELF.STATISTICS, SELF.index_concurrency, SELF.constraint_concurrency,
                SELF.table_owner, SELF.table_name, SELF.partition_name, SELF.staging_owner, SELF.staging_table,
                SELF.staging_column, SELF.index_regexp, SELF.index_type, SELF.partition_type, SELF.constraint_regexp,
                SELF.constraint_type, SELF.mapping_type, SELF.drop_dependent_objects, SELF.restartable
           FROM mapping_conf
          WHERE mapping_name = SELF.mapping_name;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_mapping', p_mapping );
      END;

      -- confirm the properties of the mapping
      verify;

      RETURN;
   END mapping_ot;

   MEMBER PROCEDURE REGISTER( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.register' );
   BEGIN
      -- store the mapping name
      SELF.mapping_name    := p_mapping;

      -- store the batch_id
      -- used to have this only set the batch_id if it was explicitly set
      -- I changed that to set it every time
      -- this also came with the setting to have P_BATCH_ID also on the END_MAPPING procedure
      td_inst.batch_id( p_batch_id );

      -- reset the evolve_object
      o_ev.clear_app_info;
   END REGISTER;

   MEMBER PROCEDURE verify
   IS
      l_src_part       BOOLEAN;
      l_tab_part       BOOLEAN;

      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.verify' );
   BEGIN
      
      evolve.log_variable( 'SELF.drop_dependent_objects', SELF.drop_dependent_objects );

      -- if the table name is not null
      -- then that means the mapping is associated with a table
      -- and we will most likely have things to do on that table
      IF SELF.table_name IS NOT NULL
      THEN

         -- make sure we also have a table owner
         IF self.table_owner IS NULL
         THEN
            evolve.raise_err( 'group_parms', 'P_OWNER and P_TABLE' );
         END IF;
         
         -- when a table_name is specified, check to make sure it exists
         td_utils.check_table( p_owner => SELF.table_owner, p_table => SELF.table_name );
         
         -- let's find out if it's partitioned
         l_tab_part      := td_utils.is_part_table( table_owner, table_name);

      END IF;
      
      -- if staging object is not null, then there are two possibilites
      -- we are either affecting indexes on TABLE based on this staging
      -- or, we are doing a table replace of some sorts
      -- this is determined by REPLACE_METHOD... if it's null, then only indexes are affected
      IF SELF.staging_table IS NOT NULL
      THEN
         
         -- make sure we also have a regular table in place
         IF self.table_owner IS NULL
         THEN
            evolve.raise_err( 'parms_group', 'P_STAGING_TABLE and P_TABLE' );
         END IF;
         
         -- make sure the table name has an owner
         IF self.staging_owner IS NULL
         THEN
            evolve.raise_err( 'parms_group', 'P_STAGING_OWNER and P_STAGING_TABLE' );
         END IF;
         
         -- when a staging object is specified, check to make sure it exists
         td_utils.check_object( p_owner            => SELF.staging_owner,
                                p_object           => SELF.staging_table,
                                p_object_type      => 'table|view'
                              );
         
         -- now, depending on REPLACE_METHOD, we are either affecting indexes only or doing a replace
         IF replace_method = 'exchange' AND mapping_type = 'table'
         THEN
   
            -- we need to find out whether staging object is partitioned or not         
            l_src_part      := td_utils.is_part_table( staging_owner, staging_table);
   
            IF l_src_part AND l_tab_part
            THEN
               evolve.raise_err( 'both_part' );
            ELSIF (NOT l_src_part AND NOT l_tab_part)
            THEN
               evolve.raise_err( 'neither_part' );
            END IF;
            
         END IF;
         
      ELSE
         
         -- staging object is null, but we want to exchange or rename
         -- that's not cool
         IF replace_method IS NOT NULL 
            AND self.mapping_type = 'table'
         THEN
            evolve.raise_err('no_source_tab');
         END IF;
      
      END IF;
      
      -- if exchange_method is 'rename' then a table rename is used
      -- in that case, the owner and source_owner need to be the same
      IF SELF.replace_method = 'rename' AND SELF.staging_owner <> SELF.table_owner AND SELF.mapping_type = 'table'
      THEN
         evolve.raise_err
            ( 'parm_not_supported',
              'A REPLACE_METHOD value of ''rename'' requires that TABLE_OWNER and STAGING_OWNER must be the same'
            );
      END IF;

      evolve.log_msg( 'Mapping confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   MEMBER PROCEDURE unusable_indexes
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.unusable_indexes' );
   BEGIN
      
      evolve.log_variable( 'SELF.manage_indexes', SELF.manage_indexes );

      -- we want to manage indexes, but there is no table replace that is occurring
      -- so we need to explicitly manage the indexes
      -- so we need to mark indexes unusable beforehand
      IF lower(self.manage_indexes) IN ('unusable','both') 
         AND NVL(self.replace_method,'NA') NOT IN ('exchange','rename')
      THEN
         td_dbutils.unusable_indexes( p_owner              => SELF.table_owner,
                                      p_table              => SELF.table_name,
                                      p_partname           => SELF.partition_name,
                                      p_source_owner       => SELF.staging_owner,
                                      p_source_object      => SELF.staging_table,
                                      p_source_column      => SELF.staging_column,
                                      p_index_regexp       => SELF.index_regexp,
                                      p_index_type         => SELF.index_type,
                                      p_part_type          => SELF.partition_type
                                    );
      END IF;

      o_ev.clear_app_info;
   END unusable_indexes;
   
   MEMBER PROCEDURE disable_constraints
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.disable_constraints' );
   BEGIN
      
      evolve.log_variable( 'SELF.manage_constraints', SELF.manage_constraints );

      -- we want to manage constraints, but there is no table replace that is occurring
      -- so we need to explicitly manage constraints
      -- so we need to disable constraints beforehand
      IF lower(self.manage_constraints) IN ('disable','both') 
         AND NVL(self.replace_method,'NA') NOT IN ('exchange','rename')
      THEN
         td_dbutils.constraint_maint( p_owner                  => SELF.table_owner,
                                      p_table                  => SELF.table_name,
                                      p_constraint_regexp      => SELF.constraint_regexp,
                                      p_constraint_type        => SELF.constraint_type,
                                      p_maint_type             => 'disable'
                                    );
      END IF;
      o_ev.clear_app_info;
   END disable_constraints;

   MEMBER PROCEDURE pre_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.pre_map' );
   BEGIN
      
      disable_constraints;
      unusable_indexes;
      
      o_ev.clear_app_info;
   END pre_map;
   
   MEMBER PROCEDURE replace_table
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.replace_table' );
   BEGIN
                  
      CASE
         WHEN SELF.replace_method = 'exchange'
         THEN
            -- partition exchange the staging table into the max partition of the target table
            -- this requires that the dimension table is a single partition table
            
            evolve.log_variable( 'SELF.drop_dependent_objects', SELF.drop_dependent_objects );

            td_dbutils.exchange_partition( p_source_owner      => SELF.staging_owner,
                                           p_source_table      => SELF.staging_table,
                                           p_owner             => SELF.table_owner,
                                           p_table             => SELF.table_name,
                                           p_partname          => SELF.partition_name,
                                           p_statistics        => SELF.STATISTICS,
                                           p_idx_concurrency   => SELF.index_concurrency,
                                           p_con_concurrency   => SELF.constraint_concurrency,
                                           p_drop_deps         => SELF.drop_dependent_objects
                                         );
         WHEN SELF.replace_method = 'rename' AND NOT evolve.is_debugmode
         THEN
            -- switch the two tables using rename
            -- requires that the tables both exist in the same schema
            td_dbutils.replace_table( p_owner             => SELF.table_owner,
                                      p_table             => SELF.table_name,
                                      p_source_table      => SELF.staging_table,
                                      p_statistics        => SELF.STATISTICS,
                                      p_idx_concurrency   => SELF.index_concurrency,
                                      p_con_concurrency   => SELF.constraint_concurrency
                                    );

            -- only drop dependent objects if desired
            IF td_core.is_true( self.drop_dependent_objects )
            THEN
               
               -- drop constraints on the stage table
               evolve.log_msg( 'Dropping constraints on the staging table', 4 );
               
               BEGIN
                  td_dbutils.drop_constraints( p_owner => SELF.staging_owner, 
                                               p_table => SELF.staging_table
                                             );
               EXCEPTION
                  WHEN td_dbutils.drop_iot_key
                  THEN
                     NULL;
               END;

               -- drop indexes on the staging table
               evolve.log_msg( 'Dropping indexes on the staging table', 4 );
               td_dbutils.drop_indexes( p_owner => SELF.staging_owner, 
                                        p_table => SELF.staging_table
                                      );
                     
            END IF;

         WHEN SELF.replace_method = 'rename' AND evolve.is_debugmode
         THEN
            evolve.log_msg( 'Cannot simulate a REPLACE_METHOD of "rename" when in DEBUGMODE', 4 );
   
         WHEN SELF.replace_method = 'merge'
         THEN
            -- switch the two tables using rename
            -- requires that the tables both exist in the same schema
            td_dbutils.merge_table( p_owner             => SELF.table_owner,
                                    p_table             => SELF.table_name,
                                    p_source_owner      => self.staging_owner,
                                    p_source_object     => SELF.staging_table
                                  );
            COMMIT;
         ELSE
            NULL;
      END CASE;
      
      o_ev.clear_app_info;
   END replace_table;

   MEMBER PROCEDURE usable_indexes
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.usable_indexes' );
   BEGIN
                  
      -- if there is no replace method, then we need to rebuild indexes
      -- rebuild the indexes
      IF lower(self.manage_indexes) IN ('usable','both') 
         AND NVL(self.replace_method,'NA') NOT IN ('rename','exchange')
      THEN
         td_dbutils.usable_indexes( p_owner           => SELF.table_owner,
                                    p_table           => SELF.table_name,
                                    p_concurrent      => SELF.index_concurrency,
                                    p_index_regexp    => SELF.index_regexp,
                                    p_index_type      => SELF.index_type
                                  );
      END IF;

      o_ev.clear_app_info;
   END usable_indexes;
   
   MEMBER PROCEDURE enable_constraints
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.enable_constraints' );
   BEGIN
                  
      -- enable the constraints
      -- if there is no replace method, then we need to enable constraints
      IF lower(self.manage_constraints) IN ('enable','both') 
         AND NVL(self.replace_method,'NA') NOT IN ('rename','exchange')
      THEN
         td_dbutils.constraint_maint( p_owner                  => SELF.table_owner,
                                      p_table                  => SELF.table_name,
                                      p_constraint_regexp      => SELF.constraint_regexp,
                                      p_constraint_type        => SELF.constraint_type,
                                      p_maint_type             => 'enable',
                                      p_concurrent	       => SELF.constraint_concurrency
                                    );

      END IF;

      o_ev.clear_app_info;
   END enable_constraints;
   
   MEMBER PROCEDURE gather_stats
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.gather_stats' );
   BEGIN
                  
      -- this is not a segment-switching situation
      -- there is a table name specified
      -- 'gather' is specified for statistics
      IF self.replace_method NOT IN ('exchange','rename')
         AND self.table_name IS NOT NULL 
         AND REGEXP_LIKE( 'gather', self.statistics, 'i' )
      THEN
         td_dbutils.gather_stats( p_owner               => self.table_owner, 
                                  p_segment             => self.table_name, 
                                  p_segment_type        => 'table' );
      END IF;

      o_ev.clear_app_info;
   END gather_stats;
   
   MEMBER PROCEDURE post_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.post_map' );
   BEGIN
                  
      replace_table;      
      usable_indexes;
      enable_constraints;
      gather_stats;

      o_ev.clear_app_info;
   END post_map;
   
   MEMBER PROCEDURE start_map
   AS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping '||SELF.mapping_name, p_action => 'start mapping' );
   BEGIN
      
      evolve.log_msg( 'Pre-mapping processes beginning' );
      
      pre_map;
      o_ev.change_action( 'execute mapping' );
      
      evolve.log_msg( 'Pre-mapping processes completed' );

   END start_map;

   MEMBER PROCEDURE end_map
   AS
   o_ev   evolve_ot := evolve_ot( p_module => 'mapping '||SELF.mapping_name, p_action=>'end mapping' );
   BEGIN
      evolve.log_msg( 'Post-mapping processes beginning' );

      post_map;

      evolve.log_msg( 'Post-mapping processes completed' );

      o_ev.clear_app_info;
   END end_map;

   -- null procedure for polymorphism only
   MEMBER PROCEDURE confirm_dim_cols
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'mapping_ot.confirm_dim_cols' );
   BEGIN
      -- simply raise an exception if this procedure ever gets called
      -- it never should, as it is only here for inheritance
      evolve.raise_err( 'wrong_map_type' );
      o_ev.clear_app_info;
   END confirm_dim_cols;
END;
/

SHOW errors