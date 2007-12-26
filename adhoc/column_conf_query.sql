SET echo off
SET feedback off
SET timing off

var p_table VARCHAR2(30)
var p_owner VARCHAR2(30)
var p_surrogate VARCHAR2(30)
var p_nat_key VARCHAR2(2000)
var p_scd1 VARCHAR2(2000)
var p_scd2 VARCHAR2(2000)
var p_effective_dt VARCHAR2(2000)
var p_expiration_dt VARCHAR2(2000)
var p_current_ind VARCHAR2(30)

EXEC :p_table := 'customer_dim';
EXEC :p_owner := 'whdata';
EXEC :p_surrogate := 'customer_dim_id';
EXEC :p_nat_key := 'shipto_nbr,source_system_cd';
EXEC :p_scd1 := NULL;
EXEC :p_scd2 := NULL;
EXEC :p_effective_dt := 'effective_start_dt';
EXEC :p_expiration_dt := 'effective_end_dt';
EXEC :p_current_ind := 'current_record_flg';


SET feedback on
SET echo on
SET timing on

SELECT *
  FROM ( 
	 SELECT owner, table_name, column_name, 'surrogate key' column_type
	   FROM  all_tab_columns
	  WHERE column_name = upper(:p_surrogate)
		UNION
	 SELECT owner, table_name, column_name, 'effective date' column_type
	   FROM  all_tab_columns
	  WHERE column_name = upper(:p_effective_dt)
		UNION
	 SELECT owner, table_name, column_name, 'expiration date' column_type
	   FROM  all_tab_columns
	  WHERE column_name = upper(:p_expiration_dt)
		UNION
	 SELECT owner, table_name, column_name, 'current indicator' column_type
	   FROM  all_tab_columns
	  WHERE column_name = upper(:p_current_ind)
		UNION
	 SELECT owner, table_name, column_name, 'natural key' column_type
	   FROM all_tab_columns atc
	   JOIN TABLE(td_core.split(upper(:p_nat_key),',')) s
		ON atc.column_name = s.column_value 
		UNION
	 SELECT owner, table_name, column_name, 'scd type 1' column_type
	   FROM all_tab_columns atc
	   JOIN TABLE(td_core.split(upper(:p_scd1),',')) s
		ON atc.column_name = s.column_value 
       )
  JOIN dimension_conf
       using (owner, table_name)
 WHERE owner = upper( :p_owner )
   AND table_name = upper( :p_table )
