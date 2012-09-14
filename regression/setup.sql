SET autotrace off
SET echo on

-- do some resetting
CONNECT stewart@db11gl/suttree1 AS sysdba

DROP USER td_demo CASCADE;

create user td_demo identified by password
DEFAULT TABLESPACE users
QUOTA UNLIMITED ON users
/

GRANT CONNECT, RESOURCE TO td_demo
/
grant select any table to td_demo
/

-- test case is reset


-- register any executing user with Transcend
EXEC tdsys.td_adm.register_user('stewart');


CONNECT stewart@db11gl/suttree1

-- use Transcend to build the test case to demonstrate low-level calls
-- build a fact table to use

BEGIN
   trans_etl.build_table 
   ( p_owner         => 'td_demo',
     p_table         => 'sales_fact',
     p_source_owner  => 'sh',
     p_source_table  => 'sales',
     p_tablespace    => 'users',
     -- could remove partitioning with "ignore"
     p_partitioning  => 'keep',
     -- bring the data over
     p_rows          => 'yes',
     -- could have brought all constraints with "yes"
     p_constraints   => 'no',
     -- could have brought all indexes with "yes"
     p_indexes       => 'no',
     -- could "gather" stats instead of transfering
     p_statistics    => 'transfer'
   );
END;
/

COMMIT;


-- build bitmap indexes on the table
BEGIN
   trans_etl.build_indexes 
   ( p_table           => 'sales_fact',
     p_owner           => 'td_demo',
     p_source_table    => 'sales',
     p_source_owner    => 'sh',
     p_index_type      => 'bitmap'
     -- could have brought only matching indexes with P_INDEX_REGEXP
   );
END;
/


-- build some constraints

BEGIN
   trans_etl.build_constraints 
   ( p_table           => 'sales_fact',
     p_owner           => 'td_demo',
     p_source_table    => 'sales',
     p_source_owner    => 'sh',
     p_constraint_type => 'c'
     -- could have brought only matching indexes with P_CONSTRAINT_REGEXP
   );
END;
/


ALTER TABLE td_demo.sales_fact parallel nologging;

create table td_demo.sales_stg 
       as select * 
            from td_demo.sales_fact partition (SALES_Q4_2001);

COMMIT;

EXEC dbms_stats.gather_table_stats( ownname => 'TD_DEMO', tabname => 'SALES_STG');


-- build a table to use as our dimension table
-- only bringing a subset of columns for simplicity

CREATE TABLE td_demo.product_dim
       AS SELECT prod_id product_key,
                 prod_id,
                 prod_name,
                 prod_desc,
                 prod_status,
                 prod_eff_from,
                 prod_eff_to,
                 prod_valid
            FROM sh.products;

-- tweak the data for SCD use
update td_demo.product_dim
   set prod_eff_to='12/31/9999',
       prod_valid='Y'
 where prod_valid='A';

COMMIT;

update td_demo.product_dim
   set prod_valid='N'
 where prod_valid='I';

COMMIT;


-- build indexes on the dimension table

BEGIN
   trans_etl.build_indexes
   ( p_owner        => 'td_demo',
     p_table        => 'product_dim',
     p_source_owner => 'sh',
     p_source_table => 'products',
     p_index_type   => 'bitmap'
   );
END;
/


-- create a seequence for the surrogate key
CREATE SEQUENCE td_demo.product_key_seq CACHE 20;


-- setup files demo

CREATE OR REPLACE directory td_source AS '/home/oracle/source_files';

CREATE OR REPLACE directory td_files AS '/home/oracle/files';

CONNECT stewart@db11gl/suttree1 AS sysdba

GRANT READ, WRITE ON directory td_source TO stewart;

GRANT READ, WRITE ON directory td_files TO stewart;


-- register directories with Transcend
-- this is required because Java Stored Procedures are used
-- this handles setting Java permissions on the directory

EXEC tdsys.td_adm.register_directory('td_source','tdrep','stewart');

EXEC tdsys.td_adm.register_directory('td_files','tdrep','stewart');

CONNECT stewart@db11gl/suttree1

PURGE recyclebin;