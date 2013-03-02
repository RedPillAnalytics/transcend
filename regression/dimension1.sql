
-- reset

BEGIN
   trans_adm.delete_dimension
   ( p_mapping          => 'map_product_dim'
   );
EXCEPTION
WHEN others THEN NULL;
END;
/


-- create a dimension table

BEGIN
   trans_adm.create_dimension
   ( p_mapping          => 'map_product_dim',
     -- dimension table
     p_owner            => 'td_demo',
     p_table            => 'product_dim',
     -- schema for intermediate tables
     p_staging_owner    => 'td_demo',
     -- source table for Transcend
     -- target table for ETL tool
     p_source_owner     => 'td_demo',
     p_source_table     => 'product_src',
     -- sequece for the dimension
     p_sequence_owner   => 'td_demo',
     p_sequence_name    => 'product_key_seq',
     -- the default SCD type
     p_default_scd_type => 2,
     p_description      => 'load for PRODUCT_DIM',
     -- manage indexes and constraints
     p_indexes          => 'both',
     p_index_type       => 'bitmap',
     p_constraints      => 'both'
   );
END;
/

COMMIT;


SELECT * 
  FROM mapping_conf
  JOIN dimension_conf
       USING (mapping_name)
 WHERE mapping_name = 'map_product_dim';


-- specify column attributes in the dimension table
BEGIN
   trans_adm.create_dim_attribs
   ( p_mapping       => 'map_product_dim',
     p_surrogate     => 'product_key',
     p_effective_dt  => 'prod_eff_from',
     p_expiration_dt => 'prod_eff_to',
     p_current_ind   => 'prod_valid',
     p_nat_key       => 'prod_id',
     p_scd1          => 'prod_status'
   );
END;
/

COMMIT;

-- put source data into the PRODUCT_SRC table
-- this is comparable to running an ETL mapping
INSERT into td_demo.product_src
       SELECT prod_id,
              prod_name,
              prod_desc,
              prod_status,
              prod_eff_from
         FROM sh.products
        WHERE ROWNUM < 11;

-- some modifications for SCD activity
UPDATE td_demo.product_src
   SET prod_name = 'New '||prod_name;

COMMIT;

-- have a look at the configuration tables
SELECT *
  FROM column_conf
 WHERE mapping_name = 'map_product_dim';


EXEC trans_etl.start_mapping( 'map_product_dim' );


EXEC trans_etl.end_mapping( 'map_product_dim' );


-- use a partition exchange instead of a MERGE

BEGIN
   trans_adm.modify_dimension
   ( p_mapping        => 'map_product_dim',
     p_replace_method => 'exchange'
   );
END;
/


EXEC trans_etl.start_mapping( 'map_product_dim' );


EXEC trans_etl.end_mapping( 'map_product_dim' );


BEGIN
   trans_adm.modify_dim_attrib
   ( p_mapping       => 'map_product_dim',
     p_column        => 'prod_status',
     p_column_type   => 'scd type 2'
   );
END;
/

COMMIT;

-- end of demo