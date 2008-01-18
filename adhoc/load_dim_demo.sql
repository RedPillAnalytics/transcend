SET echo on
-- before we start, I need to delete configurations from previous runs
BEGIN
   trans_adm.configure_dim( p_owner=>'testdim',
			    p_table=>'customer_dim',
			    p_mode=>'delete');
END;
/


-- first, create a dimension_table
DROP TABLE testdim.customer_dim CASCADE CONSTRAINTS PURGE
/

CREATE TABLE testdim.customer_dim
       ( customer_key NUMBER,
         account_number NUMBER,
	 first_name VARCHAR2(50),
	 middle_initial VARCHAR2(1),
	 last_name  VARCHAR2(50),
         birthdate DATE,
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1)
       )
/

-- lets add some grants on the table
GRANT SELECT ON testdim.customer_dim TO reporting
/

-- add the primary key on the dimension table, which is a surrogate key
ALTER TABLE testdim.customer_dim ADD CONSTRAINT customer_dim_pk PRIMARY KEY (customer_key)
/

-- create the sequence which will be used for the surrogate key
DROP SEQUENCE testdim.customer_dim_seq
/
CREATE SEQUENCE testdim.customer_dim_seq START WITH 21
/

-- populate the dimesnion table with some already existing records
insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', NULL, 'Smith', '01/03/1975', 30328, NULL,
	 '01/01/2006','01/31/2006','N')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', 'A', 'Smith', '01/03/1975', 30075, NULL,
	 '01/31/2006','02/28/2006','N')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', 'D', 'Smith', '01/03/1975', 30075, NULL,
	 '02/28/2006','12/31/9999','Y')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1039242, 'Jane', 'S', 'Smith', '07/26/1975', 30328, NULL,
	 '01/01/2006','12/31/9999','Y')
/

-- create indexes on the dimension table to replicate a real life environment
CREATE BITMAP INDEX testdim.customer_dim_bi1 on testdim.customer_dim (first_name)
/

CREATE BITMAP INDEX testdim.customer_dim_bi2 on testdim.customer_dim (last_name)
/

CREATE BITMAP INDEX testdim.customer_dim_bi3 on testdim.customer_dim (effect_start_dt)
/

CREATE BITMAP INDEX testdim.customer_dim_bi4 on testdim.customer_dim (effect_end_dt)
/

CREATE BITMAP INDEX testdim.customer_dim_bi5 on testdim.customer_dim (current_ind)
/

-- now, let's create a fact table to complete the model
DROP TABLE testdim.sales_fact CASCADE CONSTRAINTS PURGE
/

CREATE TABLE testdim.sales_fact
       ( customer_key NUMBER,
	 quantity NUMBER,
	 amount NUMBER)
/

-- add the foreign key constraint that references the dimension table
ALTER TABLE testdim.sales_fact ADD CONSTRAINT sales_fact_fk1 FOREIGN KEY (customer_key) REFERENCES testdim.customer_dim (customer_key)
/

-- insert the fact records
insert into testdim.sales_fact VALUES (21, 4, 37.49);
insert into testdim.sales_fact VALUES (21, 5, 50.99);
insert into testdim.sales_fact VALUES (22, 3, 22.14);
insert into testdim.sales_fact VALUES (23, 12, 117.55);


-- create a source table and populate it with records
-- these are the new changes that need to be applied to the dimension table
DROP TABLE testdim.customer_src purge
/

CREATE TABLE testdim.customer_src
       ( account_number NUMBER,
	 first_name VARCHAR2(50),
	 middle_initial VARCHAR2(1),
	 last_name VARCHAR2(50),
         birthdate DATE,
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE
       )
/

-- notice that the source table has all the same values as the dimension table
-- except the surrogate key (dimension_key), the expiration date (effect_end_dt), and the current indicator (current_ind)

insert into testdim.customer_src VALUES 
       (1033477, 'John', 'H', 'Smith', '01/03/1975', 30066, NULL,'03/04/2006')
/

insert into testdim.customer_src VALUES 
       (1033477, 'John', 'H', 'Smith', '01/04/1975', 30066,3333,'03/10/2006')
/

insert into testdim.customer_src VALUES 
       (1039242, 'Jane', 'B', 'Smith', '07/26/1975', NULL, NULL,'03/04/2006')
/

insert into testdim.customer_src VALUES 
       (1039242, 'Jane', 'B', 'Smith', '09/26/1980', NULL, NULL, SYSDATE)
/


-- let's have a look at our dimension and source tables
SELECT * FROM testdim.customer_dim
/

SELECT * FROM testdim.customer_src
/


-- I want to register this table with Transcend as a dimension table
BEGIN
   trans_adm.configure_dim( p_owner=>'testdim',
			    p_table=>'customer_dim',
			    p_source_owner=>'testdim',
			    p_source_object=>'customer_src',
			    p_sequence_owner=>'testdim',
			    p_sequence_name=>'customer_dim_seq',
			    p_default_scd_type=> 2,
			    p_replace_method=>'rename',
			    p_statistics=>'transfer',
			    p_concurrent=>'no');
END;
/

-- all this really entails is an entry in a configuration table
-- using T Kyte's PRINT package to make this a little easier to see
exec print.tbl(q'|select * from dimension_conf where owner='TESTDIM' and table_name='CUSTOMER_DIM'|');

-- now, configure the columns for the dimension
BEGIN
   trans_adm.configure_dim_cols ( p_owner=>'testdim',
				  p_table=>'customer_dim',
				  p_surrogate=>'customer_key',
				  p_nat_key=>'account_number',
				  p_scd1=>'birthdate',
				  p_effective_dt=>'effect_start_dt',
				  p_expiration_dt=>'effect_end_dt',
				  p_current_ind=>'current_ind');
END;
/
-- a new entry is created in the COLUMN_CONF table for every column of the specified table
-- if a column is not specified, then it defaults to an SCD of type DEFAULT_SCD_TYPE
select column_name, column_type from column_conf WHERE owner='TESTDIM' AND table_name='CUSTOMER_DIM';

-- now, execute the load
BEGIN
   trans_etl.load_dim( p_owner=>'testdim',
		       p_table=>'customer_dim');
END;
/
-- a table rename was used to replace one table with another
-- also, notice that a table was created to load the records into
-- the dimension table was replaced with this one
-- afterwards the extra table is dropped.

-- now let's see the contents of the dimension table
SELECT * FROM testdim.customer_dim
/

-- lets update the logging_level and get a better idea of what's going on
BEGIN
   evolve_adm.set_logging_level( p_module=> 'default',
				 p_logging_level=>3 );
END;
/

-- run the load again
BEGIN
   trans_etl.load_dim( p_owner=>'testdim',
		       p_table=>'customer_dim');
END;
/

-- put the logging level back
BEGIN
   evolve_adm.set_logging_level( p_module=> 'default',
				 p_logging_level=>1 );
END;
/


-- but suppose we want to use a permanent staging table
-- this would allow us to have a copy of the dimension table prior to the run
-- just need to create the table and register it with Transcend
DROP TABLE testdim.customer_scd CASCADE CONSTRAINTS PURGE
/

CREATE TABLE testdim.customer_scd
       ( customer_key NUMBER,
         account_number NUMBER,
	 first_name VARCHAR2(50),
	 middle_initial VARCHAR2(1),
	 last_name  VARCHAR2(50),
         birthdate DATE,
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1)
       )
/

BEGIN
   trans_adm.configure_dim( p_owner=>'testdim',
			    p_table=>'customer_dim',
			    p_staging_owner=>'testdim',
			    p_staging_table=>'customer_scd');
END;
/

-- now execute the load again
BEGIN
   trans_etl.load_dim( p_owner=>'testdim',
		       p_table=>'customer_dim');
END;
/

-- we still have a previous version of the dimension table
SELECT * FROM testdim.customer_scd
/

-- now, suppose that building the indexes and constraints are the largest bottleneck for the load
-- currently, the indexes and constraints build sequentially... one after another
-- let's just tell Transcend that we want to build these objects concurrently
-- and instead of transfering statistics from one table to the other, let's gather statistics
-- Transcend uses all the default values of DBMS_STATS introduced in 10g for automatics stats collection
-- it will figure out the best values for granularity, percentage, parallelism, etc.
BEGIN
   trans_adm.configure_dim( p_owner=>'testdim',
			    p_table=>'customer_dim',
			    p_concurrent=>'yes',
			    p_statistics=>'gather');
END;
/

-- now, let's do the load again
BEGIN
   trans_etl.load_dim( p_owner=>'testdim',
		       p_table=>'customer_dim');
END;
/

-- now, suppose we want to use partition exchanging instead of table renaming
-- this gives us near-100% availability
-- also allows for the staging table to exist in another schema

-- this requires a single-partition dimension table to work correctly
-- also creating it index-organized to demonstrate support for that
DROP TABLE testdim.customer_dim CASCADE CONSTRAINTS PURGE
/

CREATE TABLE testdim.customer_dim
       ( customer_key NUMBER,
         account_number NUMBER,
	 first_name VARCHAR2(50),
	 middle_initial VARCHAR2(1),
	 last_name  VARCHAR2(50),
         birthdate DATE,
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1),
       CONSTRAINT CUSTOMER_DIM_PK PRIMARY KEY (CUSTOMER_KEY)       
)
    ORGANIZATION INDEX 
         MAPPING TABLE
     PARTITION BY RANGE (CUSTOMER_KEY) 
(  
  PARTITION MAX VALUES LESS THAN (MAXVALUE)
)
/

-- create the sequence which will be used for the surrogate key
DROP SEQUENCE testdim.customer_dim_seq
/
CREATE SEQUENCE testdim.customer_dim_seq START WITH 21
/


GRANT SELECT ON testdim.customer_dim TO reporting
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', NULL, 'Smith', '01/03/1975', 30328, NULL,
	 '01/01/2006','01/31/2006','N')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', 'A', 'Smith', '01/03/1975', 30075, NULL,
	 '01/31/2006','02/28/2006','N')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1033477, 'John', 'D', 'Smith', '01/03/1975', 30075, NULL,
	 '02/28/2006','12/31/9999','Y')
/

insert into testdim.customer_dim VALUES 
       (testdim.customer_dim_seq.nextval, 1039242, 'Jane', 'S', 'Smith', '07/26/1975', 30328, NULL,
	 '01/01/2006','12/31/9999','Y')
/

CREATE BITMAP INDEX testdim.customer_dim_bi1 on testdim.customer_dim (first_name) local
/

CREATE BITMAP INDEX testdim.customer_dim_bi2 on testdim.customer_dim (last_name) local
/

CREATE BITMAP INDEX testdim.customer_dim_bi3 on testdim.customer_dim (effect_start_dt) local
/

CREATE BITMAP INDEX testdim.customer_dim_bi4 on testdim.customer_dim (effect_end_dt) local
/

CREATE BITMAP INDEX testdim.customer_dim_bi5 on testdim.customer_dim (current_ind) local
/

ALTER TABLE testdim.sales_fact ADD CONSTRAINT sales_fact_fk1 FOREIGN KEY (customer_key) REFERENCES testdim.customer_dim (customer_key)
/

-- the staging table should not be partitioned
-- it will also have to be index-organized
-- exchanging a partition requires like table definitions
DROP TABLE testdim.customer_scd
/

CREATE TABLE testdim.customer_scd
       ( customer_key NUMBER,
         account_number NUMBER,
	 first_name VARCHAR2(50),
	 middle_initial VARCHAR2(1),
	 last_name  VARCHAR2(50),
         birthdate DATE,
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1),
       CONSTRAINT CUSTOMER_SCD_PK PRIMARY KEY (CUSTOMER_KEY)
       )
    ORGANIZATION INDEX 
         MAPPING TABLE
/

-- now register our preferred replace method with Transcend
BEGIN
   trans_adm.configure_dim( p_owner=>'testdim',
			    p_table=>'customer_dim',
			    p_replace_method=>'exchange');
END;
/

-- now, let's do the load again
BEGIN
   trans_etl.load_dim( p_owner=>'testdim',
		       p_table=>'customer_dim');
END;
/

