CREATE OR REPLACE TYPE BODY cdc_group_ot
AS

   -- constructor function for the CDC_GROUP_OT object type
   CONSTRUCTOR FUNCTION cdc_group_ot 
   ( 
     p_group_name VARCHAR2
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      
      -- populate the group information
      self.initialize ( p_group_name );

      -- return the self reference
      RETURN;

   END cdc_group_ot;
   
   MEMBER PROCEDURE initialize
   ( 
     p_group_name VARCHAR2
   )
   IS
      l_rows    BOOLEAN     := FALSE;
      -- evolve object
      o_ev      evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.initialize');
   BEGIN

      BEGIN
         
         -- load all attributes relatd to group into the type
         SELECT source_type,
                group_name,
                subscription,
                nvl(interface, subscription),
                filter_policy,
                interface_prefix,
                dblink_name,
                initial_source_scn
           INTO self.source_type,
                self.group_name,
                self.subscription,
                self.interface,
                self.filter_policy,
                self.interface_prefix,
                self.dblink_name,
                self.initial_source_scn
           FROM cdc_source
           JOIN cdc_group
                USING (source_name)
          WHERE lower( group_name ) = lower( p_group_name );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_cdc_group', p_group_name );

      END;

      SELECT nvl( source_scn, 'source_scn'),
             nvl( commit_date, 'commit_date'),
             nvl( source_maxscn, 'source_maxscn'),
             nvl( source_minscn, 'source_minscn'),
             nvl( row_rank, 'row_rank'),
             nvl( cdc_rank, 'cdc_rank'),
             nvl( entity_rank, 'entity_rank'),
             nvl( dml_type, 'dml_type')
        INTO self.source_scn,
             self.commit_date,
             self.source_maxscn,
             self.source_minscn,
             self.row_rank,
             self.cdc_rank,
             self.entity_rank,
             self.dml_type
        FROM (
               SELECT column_type,
                      column_name
                 FROM cdc_audit_datatype
                WHERE group_name = self.group_name
             )
             pivot
             ( MAX(column_name) FOR column_type IN (
                                                     'source_scn' AS source_scn,
                                                     'commit_date' AS commit_date,
                                                     'source_maxscn' AS source_maxscn,
                                                     'source_minscn' AS source_minscn,
                                                     'row_rank' AS row_rank,
                                                     'cdc_rank' AS cdc_rank,
                                                     'entity_rank' AS entity_rank,
                                                     'dml_type' AS dml_type                                                     
                                                   )
             );
      

   END initialize;

   MEMBER PROCEDURE register_initial_scn
   ( 
     p_scn        NUMBER
   )
   IS
      l_rows    BOOLEAN     := FALSE;
      -- evolve object
      o_ev      evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.register_initial_scn');
   BEGIN
      
      self.initial_source_scn := p_scn;

      UPDATE cdc_group
         SET initial_source_scn = p_scn
       WHERE group_name = self.group_name;      

   END register_initial_scn;
   
   MEMBER FUNCTION get_source_scn
      RETURN NUMBER
   AS

      l_scn_sql    VARCHAR2(4000);
      l_scn        NUMBER;
   
      o_ev         evolve_ot           := evolve_ot (p_module => 'cdc_group_ot.get_source_scn');
   BEGIN      

      -- use the dblink to get this
      
      l_scn_sql :=
      q'{ select current_scn from v$database@}'
      || self.dblink_name;

      evolve.log_variable( 'l_scn_sql', l_scn_sql );
      
      EXECUTE IMMEDIATE l_scn_sql
      INTO l_scn;
      
      evolve.log_variable( 'l_scn', l_scn );
      -- reset the evolve_object
      o_ev.clear_app_info;

      RETURN l_scn;

   END get_source_scn;

   MEMBER FUNCTION get_entityrank_clause
      (
        p_natkey  VARCHAR2
      )

      RETURN VARCHAR2 
   AS

      l_entityrank      VARCHAR2(4000);
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.get_entityrank_clause');
   BEGIN

      l_entityrank := 
                q'<DENSE_RANK() over ( PARTITION by >'
             || p_natkey
             || q'< ORDER BY >'
             || self.source_scn
             || q'< DESC) >'
             || self.entity_rank;

      evolve.log_variable( 'l_entityrank',l_entityrank );

      -- reset the evolve_object
      o_ev.clear_app_info;      

      RETURN l_entityrank;
      
   END get_entityrank_clause;

   MEMBER FUNCTION get_cdcrank_clause
      (
        p_natkey  VARCHAR2
      )

      RETURN VARCHAR2 
   AS

      l_cdcrank      VARCHAR2(4000);
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.get_cdcrank_clause');
   BEGIN

      l_cdcrank := 
                q'<DENSE_RANK() over ( PARTITION by >'
             || p_natkey
             || ','
             || self.source_scn
             || q'< ORDER BY >'
             || self.row_rank
             || q'< DESC) >'
             || self.cdc_rank;

      evolve.log_variable( 'l_cdcrank',l_cdcrank );

      -- reset the evolve_object
      o_ev.clear_app_info;
      
      RETURN l_cdcrank;
      
   END get_cdcrank_clause;

   MEMBER FUNCTION get_expiration_clause
      (
        p_natkey  VARCHAR2
      )

      RETURN VARCHAR2 
   AS

      l_expiration      VARCHAR2(4000);
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.get_expiration_clause');
   BEGIN

      l_expiration :=
                q'<NVL( LEAD( >'
             || self.source_scn
             || q'< ) over ( PARTITION by >'
             || p_natkey
             || q'< ORDER BY >'
             || self.source_scn
             || q'< ), >'
             || self.source_scn
             || q'< ) >'
             || self.source_maxscn;

      evolve.log_variable( 'l_expiration',l_expiration );

      -- reset the evolve_object
      o_ev.clear_app_info;      

      RETURN l_expiration;
      
   END get_expiration_clause;
   
   MEMBER FUNCTION get_join_select
   (
     p_table    VARCHAR2,
     p_natkey   VARCHAR2,
     p_collist  VARCHAR2,
     p_collapse BOOLEAN
    )
      RETURN VARCHAR2
   AS

      l_sql          VARCHAR2(4000);
      o_ev           evolve_ot          := evolve_ot (p_module => 'cdc_group_ot.get_join_select');
   BEGIN

      l_sql :=
                '( SELECT * FROM ( SELECT sub_name, '
             || lower( p_collist )
             || ', effective_scn, expiration_scn, '
             || CASE WHEN p_collapse THEN self.get_cdcrank_clause( p_natkey ) ||', ' ELSE NULL END
             || self.get_entityrank_clause( p_natkey )
             || ' FROM '
             || lower( p_table )
             || ' JOIN cdc_subscription ON group_id = '
             || self.group_id
             || ' AND '
             || self.source_scn
             || ' <= expiration_scn ) '
             || CASE WHEN p_collapse THEN ' WHERE '|| self.cdc_rank || ' = 1' ELSE NULL END
             || ' )';

      evolve.log_variable( 'l_sql',l_sql );

      -- reset the evolve_object
      o_ev.clear_app_info;
      
      RETURN l_sql;

   END get_join_select;

   MEMBER FUNCTION get_case_select
   (
     p_table    VARCHAR2,
     p_natkey   VARCHAR2,
     p_collist  VARCHAR2,
     p_collapse BOOLEAN
   )
      RETURN VARCHAR2
   AS

      l_sql          VARCHAR2(4000);
      o_ev           evolve_ot          := evolve_ot (p_module => 'cdc_group_ot.get_case_select');
   BEGIN

      l_sql :=
                'SELECT '
             || lower( p_collist )
             || ', sub_name, CASE WHEN '
             || self.source_scn
             || q'< >= effective_scn THEN 'Y' ELSE 'N' END stage_ind, >'
             || 'CASE WHEN '
             || self.entity_rank
             || q'< = 1 then 'Y' else 'N' end current_ind, >'
             || self.source_scn
             || ' '
             || self.source_minscn
             || ', '
             || self.get_expiration_clause( p_natkey => p_natkey )
             || ' FROM '
             || self.get_join_select
                 ( p_table      => p_table, 
                   p_natkey     => p_natkey, 
                   p_collist    => p_collist, 
                   p_collapse   => p_collapse 
                 );

      evolve.log_variable( 'l_sql',l_sql );

      -- reset the evolve_object
      o_ev.clear_app_info;
      
      RETURN l_sql;

   END get_case_select;

   MEMBER FUNCTION get_ctas_statement
   (
     p_owner           VARCHAR2,
     p_table           VARCHAR2,
     p_source_owner    VARCHAR2,
     p_source_table    VARCHAR2,
     p_dblink          VARCHAR2,
     p_collist         VARCHAR2 DEFAULT NULL,
     p_rows            BOOLEAN DEFAULT TRUE
   )
      RETURN VARCHAR2
   AS
      l_collist      VARCHAR2(4000);
      l_sql          VARCHAR2(4000);

      -- target table information
      l_table           all_tables.table_name%type       := lower( p_table );
      l_towner          all_tables.owner%type            := lower( p_owner );
      l_full_table      VARCHAR2(61)                     := l_towner || '.' || l_table;

      -- source table information
      l_stable          all_tables.table_name%type       := lower( p_source_table );
      l_sowner          all_tables.owner%type            := lower( p_source_owner );
      l_full_stable     VARCHAR2(61)                     := l_sowner || '.' || l_stable;

      o_ev           evolve_ot          := evolve_ot (p_module => 'cdc_group_ot.get_ctas_statement');
   BEGIN
      
      -- pull the column list from the database if it isn't provided
      IF p_collist IS NULL
      THEN
         l_collist := td_utils.get_column_list
         ( 
           p_owner    => p_source_owner,
           p_table    => p_source_table,
           p_dblink   => p_dblink
         );
      ELSE
         l_collist := p_collist;
      END IF;
      
      evolve.log_variable( 'l_collist', l_collist );
      
      -- create a table using CTAS
      l_sql  :=
            'CREATE TABLE '
         || l_full_table
         || ' ( '
         || l_collist
         || ' ) as select '
         || l_collist
         || ' from '
      || l_full_stable
      || CASE WHEN p_dblink IS NOT NULL THEN '@'||p_dblink ELSE NULL END
      || CASE WHEN NOT p_rows THEN ' WHERE 1=0' ELSE NULL END;

      -- reset the evolve_object
      o_ev.clear_app_info;
      
      RETURN l_sql;

   END get_ctas_statement;

   MEMBER PROCEDURE build_view
   ( 
     p_table   VARCHAR2,
     p_natkey  VARCHAR2 
   )
   IS
   
      -- hold a comma-delimited list of columns
      l_collist         VARCHAR2(4000);
      
      -- table information
      l_table           all_tables.table_name%type       := lower( p_table );
      l_towner          all_tables.owner%type            := lower( self.subscription );
      l_full_table      VARCHAR2(61)                     := l_towner || '.' || l_table;

      -- view information
      l_view            all_tables.table_name%type       := lower( self.interface_prefix||p_table );
      l_vowner          all_tables.owner%type            := lower( self.interface );
      l_full_view       VARCHAR2(61)                     := l_vowner || '.' || l_view;
   
      -- construct the view SQL
      l_view_sql         VARCHAR2(4000);

      -- evolve object
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.build_view');
   BEGIN

      evolve.log_variable( 'self.filter_policy', self.filter_policy );

      evolve.log_variable( 'l_towner', l_towner );
      evolve.log_variable( 'l_table', l_table );
      evolve.log_variable( 'l_full_table', l_full_table );

      evolve.log_variable( 'l_vowner', l_vowner );
      evolve.log_variable( 'l_view', l_view );
      evolve.log_variable( 'l_full_view', l_full_view );
      
      -- get a list of columns
      l_collist := td_utils.get_column_list
      ( 
        p_owner => l_towner,
        p_table => l_table
      );

      evolve.log_variable( 'l_collist', l_collist );

      -- get the final select statement
      l_view_sql  := 'CREATE or REPLACE VIEW '
                  || l_full_view
                  || ' as '
                  || self.get_case_select
                     ( 
                       p_table      => l_full_table, 
                       p_natkey     => p_natkey, 
                       p_collist    => l_collist,
                       p_collapse   => CASE self.filter_policy 
                                       WHEN 'interface' 
                                       THEN TRUE 
                                       ELSE FALSE 
                                       END 
                    );      
      
      -- build the view
      o_ev.change_action( 'build view' );
      evolve.exec_sql( l_view_sql );

      -- reset the evolve_object
      o_ev.clear_app_info;
   END build_view;

   MEMBER PROCEDURE build_table
   ( 
     p_owner            VARCHAR2,
     p_table            VARCHAR2,
     p_source_owner     VARCHAR2,
     p_source_table     VARCHAR2,
     p_natkey           VARCHAR2,
     p_add_rows         BOOLEAN  DEFAULT FALSE,
     p_dblink           VARCHAR2 DEFAULT NULL
   )
   IS
      -- hold a comma-delimited list of columns
      l_collist         VARCHAR2(4000);
      
      -- target table information
      l_table           all_tables.table_name%type       := lower( p_table );
      l_towner          all_tables.owner%type            := lower( p_owner );
      l_full_table      VARCHAR2(61)                     := l_towner || '.' || l_table;

      -- source table information
      l_stable          all_tables.table_name%type       := lower( p_source_table );
      l_sowner          all_tables.owner%type            := lower( p_source_owner );
      l_full_stable     VARCHAR2(61)                     := l_sowner || '.' || l_stable;
   
      -- construct the view SQL
      l_ctas             VARCHAR2(4000);
   
      l_rows             BOOLEAN                         := FALSE;

     o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.load_fnd_table');
   BEGIN

      evolve.log_variable( 'self.filter_policy', self.filter_policy );

      evolve.log_variable( 'l_towner', l_towner );
      evolve.log_variable( 'l_table', l_table );
      evolve.log_variable( 'l_full_table', l_full_table );

      evolve.log_variable( 'l_sowner', l_sowner );
      evolve.log_variable( 'l_stable', l_stable );
      evolve.log_variable( 'l_full_stable', l_full_stable );
      
      -- get the ctas statement
      l_ctas := self.get_ctas_statement
      ( 
        p_owner         => p_owner,
        p_table         => p_table,
        p_source_owner  => p_source_owner,
        p_source_table  => p_source_table,
        p_dblink        => p_dblink,
        p_rows          => p_add_rows
      );
      
      evolve.log_variable( 'l_collist', l_collist );
      
      -- build the view
      o_ev.change_action( 'create table' );
      
      evolve.exec_sql( l_ctas );
      
      -- reset the evolve_object
      o_ev.clear_app_info;
   END build_table;
   
   MEMBER PROCEDURE add_audit_columns
   ( 
     p_owner            VARCHAR2,
     p_table            VARCHAR2,
     p_scn              NUMBER    DEFAULT 0,
     p_dmltype          VARCHAR2  DEFAULT 'initial load',
     p_commit_date      DATE      DEFAULT SYSDATE,
     p_rowrank          NUMBER    DEFAULT 1
   )
   IS      
      -- target table information
      l_table           all_tables.table_name%type       := lower( p_table );
      l_towner          all_tables.owner%type            := lower( p_owner );
      l_full_table      VARCHAR2(61)                     := l_towner || '.' || l_table;
   
      l_rows            BOOLEAN                          := FALSE;

     o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.load_fnd_table');
   BEGIN

      evolve.log_variable( 'l_towner', l_towner );
      evolve.log_variable( 'l_table', l_table );
      evolve.log_variable( 'l_full_table', l_full_table );
      
      -- add audit columns only if P_ADD_AUDIT is true
      FOR c_cols IN ( 
                      SELECT 'ALTER TABLE '
                             || l_full_table
                             || ' add '
                             || column_name
                             || ' '
                             || datatype
                             || ' default '
                             || CASE column_type
                                     WHEN 'source_scn' THEN to_char( p_scn )
                                     WHEN 'dml_type' THEN '''' || p_dmltype || ''''
                                     WHEN 'commit_date' THEN '''' || to_char( p_commit_date ) || ''''
                                     WHEN 'row_rank' THEN to_char( p_rowrank )
                                     --ELSE NULL
                             END DDL,
                             column_type
                        FROM cdc_audit_datatype
                       WHERE group_id = self.group_id
                         AND column_type NOT IN (
                                                  'source_maxscn',
                                                  'source_minscn',
                                                  'cdc_rank',
                                                  'entity_rank'
                                                )
                       ORDER BY CASE column_type 
                             WHEN 'source_scn' THEN 1
                             WHEN 'commit_date' THEN 2
                             WHEN 'row_rank' THEN 3
                             WHEN 'dml_type' THEN 4
                             END
                    )
      LOOP
         l_rows    := TRUE;
         
         evolve.log_variable( 'column_type', c_cols.column_type );

         evolve.exec_sql( c_cols.ddl );
         
      END LOOP;

      -- reset the evolve_object
      o_ev.clear_app_info;
   END add_audit_columns;
   
   MEMBER PROCEDURE build_interface
   IS
      l_rows    BOOLEAN     := FALSE;
      -- evolve object
      o_ev      evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.build_interface');
   BEGIN

      evolve.log_variable( 'self.filter_policy', self.filter_policy );
      
      FOR c_tables IN ( SELECT nvl( table_name, source_table ) table_name,
                               natkey_list
                          FROM cdc_entity
                         WHERE group_id = self.group_id
                      )
      LOOP
         l_rows    := TRUE;

         evolve.log_variable( 'c_tables.table_name', c_tables.table_name );
         evolve.log_variable( 'c_tables.natkey_list', c_tables.natkey_list );
         
         self.build_view
         (
           p_table      => c_tables.table_name,
           p_natkey     => c_tables.natkey_list
         );

      END LOOP;

   END build_interface;
   
   MEMBER PROCEDURE build_subscription
   (
     p_scn      NUMBER          DEFAULT NULL
   )
   IS
      l_rows    BOOLEAN     := FALSE;
   
      -- evolve object
      o_ev      evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.build_subscription');
   BEGIN

      -- register the initial SCN
      self.register_initial_scn( NVL( p_scn, get_source_scn ) );
      
      -- should not be called if there is not a subscription component defined

      evolve.log_variable( 'self.subscription', self.subscription );
      
      o_ev.change_action( 'check subscription' );

      IF self.subscription IS NULL
      THEN

         evolve.raise_err( 'no_subscription_layer' );

      END IF;

      -- build each of the tables
      o_ev.change_action( 'table cursor' );      
      FOR c_tables IN ( SELECT nvl( table_name, source_table ) table_name,
                               source_table,
                               source_owner,
                               natkey_list
                          FROM cdc_entity
                         WHERE group_id = self.group_id
                      )
      LOOP

         l_rows    := TRUE;

         evolve.log_variable( 'c_tables.table_name', c_tables.table_name );
         evolve.log_variable( 'c_tables.natkey_list', c_tables.natkey_list );

         self.build_table
         (
           p_owner         => self.subscription,
           p_table         => c_tables.table_name,
           p_source_owner  => c_tables.source_owner,
           p_source_table  => c_tables.source_table,
           p_natkey        => c_tables.natkey_list,
           p_dblink        => self.dblink_name
         );

         self.add_audit_columns
         (
           p_owner         => self.subscription,
           p_table         => c_tables.table_name,
           p_scn           => self.initial_source_scn
         );
         
      END LOOP;

   END build_subscription;

   MEMBER PROCEDURE load_subscription
   IS
      l_rows    BOOLEAN     := FALSE;
   
      -- evolve object
      o_ev      evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.load_subscription');
   BEGIN

      -- should not be called if there is not a subscription layer defined

      evolve.log_variable( 'self.subscription', self.subscription );
      
      o_ev.change_action( 'check subscription' );

      IF self.subscription IS NULL
      THEN

         evolve.raise_err( 'no_subscription_layer' );

      END IF;

      -- load each of the tables
      o_ev.change_action( 'table cursor' );      
      FOR c_tables IN ( SELECT nvl( table_name, source_table ) table_name,
                               source_table,
                               source_owner,
                               natkey_list
                          FROM cdc_entity
                         WHERE group_id = self.group_id
                      )
      LOOP

         l_rows    := TRUE;

         evolve.log_variable( 'c_tables.table_name', c_tables.table_name );
                  
         td_dbutils.insert_table
         (
           p_owner         => self.subscription,
           p_table         => c_tables.table_name,
           p_source_owner  => c_tables.source_owner,
           p_source_object => c_tables.source_table,
           p_scn           => self.initial_source_scn,
           p_dblink        => self.dblink_name
         );
         
         COMMIT;
         
      END LOOP;

   END load_subscription;
   
END;
/

SHOW errors
