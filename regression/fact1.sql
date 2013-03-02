COL "Execute?" format a10
-- reset

BEGIN
   trans_adm.delete_mapping
   ( p_mapping => 'map_sales_fact' );
END;
/


-- test basic mapping instrumentation
BEGIN
   trans_adm.create_mapping
   ( p_mapping => 'map_sales_fact' );
END;
/


SELECT * 
  FROM mapping_conf
 WHERE mapping_name = 'map_sales_fact';


-- now have a START_MAPPING call to run before the mapping
EXEC trans_etl.start_mapping( 'map_sales_fact' );


-- mapping is now instrumented
-- use DBMS_MONITOR, v$session and v$session_longops
select SYS_CONTEXT( 'USERENV', 'MODULE' ) module,
       SYS_CONTEXT( 'USERENV', 'ACTION' ) action
  from dual;


-- now have an END_MAPPING call to run after the mapping
EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- logging table for all standard behavior
-- configurable logging levels
SELECT * FROM log;


-- Test index and constraint maintenance

-- configure index load behavior
BEGIN
   trans_adm.modify_mapping
   ( p_mapping         => 'map_sales_fact',
     p_table           => 'sales_fact',
     p_owner           => 'td_demo',
     p_indexes         => 'both',
     p_index_type      => 'bitmap',
     p_idx_concurrency => 'yes'
   );
END;
/


EXEC trans_etl.start_mapping( 'map_sales_fact' );


EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- configure constraint load behavior
BEGIN
   trans_adm.modify_mapping
   ( p_mapping         => 'map_sales_fact',
     p_constraints     => 'both',
     p_constraint_type => 'c',
     p_con_concurrency => 'no'
   );
END;
/


EXEC trans_etl.start_mapping( 'map_sales_fact' );


EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- now test modifying only particular index partitions

-- see all the distinct dates in the staging table
SELECT DISTINCT to_char(time_id,'mm/yyyy') 
  FROM td_demo.sales_stg;


-- configure the mapping to have a staging table associated with it
BEGIN
   trans_adm.modify_mapping
   ( p_mapping          => 'map_sales_fact',
     p_staging_owner    => 'td_demo',
     p_staging_table    => 'sales_stg'
   );
END;
/


EXEC trans_etl.start_mapping( 'map_sales_fact' );


-- see only a subset of partitions were affected
SELECT dip.index_name,
       partition_name,
       dip.status
  FROM dba_ind_partitions dip
  JOIN dba_indexes di
       ON dip.index_owner = di.owner
   AND dip.index_name = di.index_name
 WHERE table_owner='TD_DEMO'
   AND table_name='SALES_FACT'
   AND dip.status='UNUSABLE';



EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- now do a partition exchange instead of index and constraint maintenance

BEGIN
   trans_adm.modify_mapping
   ( p_mapping          => 'map_sales_fact',
     p_replace_method   => 'exchange',
     p_partname         => 'sales_q4_2001'
   );
END;
/

COMMIT;


-- notice that there are no START_MAPPING processes executed anymore
EXEC trans_etl.start_mapping( 'map_sales_fact' );


EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- demonstrate map control

-- turn on map control
BEGIN
   trans_adm.create_map_control
   ( p_mapping => 'map_sales_fact' );
END;
/


-- use the conditional function to tell if we should run the mapping or not
SELECT trans_etl.execute_mapping_str( p_mapping => 'map_sales_fact' ) "Execute?"
  FROM dual;


EXEC trans_etl.start_mapping( 'map_sales_fact' );

EXEC trans_etl.end_mapping( 'map_sales_fact' );


-- after successfully running the mapping, check the function again
SELECT trans_etl.execute_mapping_str( p_mapping => 'map_sales_fact' ) "Execute?"
  FROM dual;


-- reset the map control at the end of the batch
EXEC trans_etl.reset_map_control;


-- see if the function result has changed
SELECT trans_etl.execute_mapping_str( p_mapping => 'map_sales_fact' ) "Execute?"
  FROM dual;

-- end of demo