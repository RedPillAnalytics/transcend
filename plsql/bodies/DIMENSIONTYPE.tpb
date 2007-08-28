CREATE OR REPLACE TYPE BODY dimensiontype
AS
   MEMBER PROCEDURE index_maint
   IS
      o_td     tdtype  := tdtype( p_module => 'index_maint' );
      l_rows   BOOLEAN DEFAULT FALSE;
   BEGIN
      o_td.change_action( 'Perform index maintenance' );

      -- open a cursor containing in information for all the index maintenance to be performed
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
   END index_maint;
   MEMBER PROCEDURE constraint_maint
   IS
      o_td     tdtype  := tdtype( p_module => 'constraint_maint' );
      l_rows   BOOLEAN DEFAULT FALSE;
   BEGIN
      o_td.change_action( 'Perform index maintenance' );

      -- open a cursor containing in information for all the index maintenance to be performed
      FOR c_idx IN ( SELECT owner, table_name, constraint_regexp, constraint_type
                      FROM constraint_maint_conf
                     WHERE owner = SELF.source_owner AND table_name = SELF.table_name )
      LOOP
         l_rows := TRUE;
         td_dbapi.disable_constraints( p_owner             => c_idx.owner,
                                       p_table             => c_idx.table_name,
                                       p_index_regexp      => c_idx.index_regexp,
                                       p_index_type        => c_idx.index_type,
                                       p_part_type         => c_idx.part_type
                                     );
      END LOOP;

      IF NOT l_rows
      THEN
         td_inst.log_msg( 'No index maintenance configured for ' || full_table );
      END IF;
   END constraint_maint;
END;
/