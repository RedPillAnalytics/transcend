CREATE OR REPLACE TYPE BODY dimension_ot
AS
   MEMBER PROCEDURE LOAD
   IS
      o_ev     evolve_ot  := evolve_ot( p_module => 'load' );
      l_rows   BOOLEAN;
   BEGIN
      -- check to see if the dimension table exists
      td_utils.check_table( p_owner => owner, p_table => table_name );
      -- check that the source object exists
      td_utils.check_object( p_owner            => source_owner,
                           p_object           => source_object,
                           p_object_type      => 'table$|view'
                         );

      -- check to see if the staging table is constant
      IF td_core.is_true( constant_staging )
      THEN
         -- if it is, then make sure that it exists
         td_utils.check_table( p_owner => staging_owner, p_table => staging_table );
      ELSE
         -- otherwise, create the table
         o_ev.change_action( 'Create staging table' );
         td_dbutils.build_table( p_source_owner      => owner,
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
      END IF;

      -- now run the insert statement to load the staging table
      o_ev.change_action( 'Load staging table' );
      evolve_app.exec_sql( load_sql );
      -- perform the replace method
      o_ev.change_action( 'Load staging table' );

      CASE replace_method
         WHEN 'exchange'
         THEN
            td_dbutils.exchange_partition( p_source_owner      => staging_owner,
                                       p_source_table      => staging_table,
                                       p_owner             => owner,
                                       p_table             => table_name,
                                       p_statistics        => 'transfer'
                                     );
         WHEN 'replace'
         THEN
            td_dbutils.replace_table( p_owner             => owner,
                                  p_table             => table_name,
                                  p_source_table      => staging_table,
                                  p_statistics        => 'transfer'
                                );
         ELSE
            NULL;
      END CASE;
   END LOAD;
END;
/

SHOW errors
