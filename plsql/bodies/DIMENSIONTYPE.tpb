CREATE OR REPLACE TYPE BODY dimensiontype
AS
   MEMBER PROCEDURE LOAD
   IS
      o_td         tdtype         := tdtype( p_module => 'load' );
      l_rows       BOOLEAN;

   BEGIN      
      
      -- create a table to use to hold the staging results of the analytics statement
      o_td.change_action('Create staging table');
      td_dbapi.build_table( p_source_owner      => owner,
                            p_source_table      => table_name,
                            p_owner             => owner,
                            p_table             => staging_table,
                            -- if the data will be replaced in using an exchange, then need the table to not be partitioned
                            -- everything else can be created just like the source table
                            p_partitioning      => CASE replace_method
                               WHEN 'exchange'
                                  THEN 'no'
                               ELSE 'yes'
                            END
                          );
      
      -- now run the insert statement to load the staging table
      o_td.change_action('Load staging table');
      
      

      -- if the replace method is a partition exchange, then no index maintenance needs to be performed
      IF replace_method <> 'exchange'
      THEN
         -- work on indexes
         o_td.change_action( 'Mark indexes unusable' );
         -- set l_rows to false
         l_rows := FALSE;

         -- open a cursor containing information for all the index maintenance calls
         FOR c_idx IN ( SELECT owner, table_name, partname, source_owner, source_object,
                               source_column, d_num, p_num, index_regexp, index_type,
                               part_type
                         FROM index_maint_conf
                        WHERE owner = SELF.source_owner AND table_name = SELF.table_name )
         LOOP
            l_rows := TRUE;
            td_dbapi.unusable_indexes( p_owner              => c_idx.owner,
                                       p_table              => c_idx.table_name,
                                       p_source_owner       => c_idx.source_owner,
                                       p_source_object      => c_idx.source_object,
                                       p_source_column      => c_idx.source_column,
                                       p_d_num              => c_idx.d_num,
                                       p_p_num              => c_idx.p_num,
                                       p_index_regexp       => c_idx.index_regexp,
                                       p_index_type         => c_idx.index_type,
                                       p_part_type          => c_idx.part_type
                                     );
         END LOOP;

         IF NOT l_rows
         THEN
            td_inst.log_msg( 'No index maintenance configured for ' || full_table );
         END IF;

         -- now work on constraints
         o_td.change_action( 'Disable constraints' );
         -- reset empty cursor variable
         l_rows := FALSE;

         -- open a cursor containing information for all the constraint maintenance calls
         FOR c_cons IN ( SELECT owner, table_name, constraint_regexp, constraint_type
                          FROM constraint_maint_conf
                         WHERE owner = SELF.source_owner AND table_name = SELF.table_name )
         LOOP
            l_rows := TRUE;
            td_dbapi.disable_constraints
                                       ( p_owner                  => c_cons.owner,
                                         p_table                  => c_cons.table_name,
                                         p_constraint_regexp      => c_cons.constraint_regexp,
                                         p_constraint_type        => c_cons.constraint_type
                                       );
         END LOOP;

         IF NOT l_rows
         THEN
            td_inst.log_msg( 'No index maintenance configured for ' || full_table );
         END IF;
      END IF;

   END LOAD;
END;
/