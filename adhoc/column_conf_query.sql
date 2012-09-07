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
EXEC :p_scd1 := 'party_id,customer_nm';
EXEC :p_scd2 := 'batch_id';
EXEC :p_effective_dt := 'effective_start_dt';
EXEC :p_expiration_dt := 'effective_end_dt';
EXEC :p_current_ind := 'current_record_flg';


SET feedback on
SET echo on
SET timing on


      -- do the first merge to update any changed column_types from the parameters
      MERGE INTO column_conf t
         USING ( SELECT *
                  FROM ( SELECT owner, table_name, column_name, 'surrogate key' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( :p_surrogate )
                        UNION
                        SELECT owner, table_name, column_name, 'effective date' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( :p_effective_dt )
                        UNION
                        SELECT owner, table_name, column_name, 'expiration date' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( :p_expiration_dt )
                        UNION
                        SELECT owner, table_name, column_name, 'current indicator' column_type
                          FROM all_tab_columns
                         WHERE column_name = UPPER( :p_current_ind )
                        UNION
                        SELECT owner, table_name, column_name, 'natural key' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( :p_nat_key ), ',' ) AS split_ot )
                                                             ) s ON atc.column_name = s.COLUMN_VALUE
                        UNION
                        SELECT owner, table_name, column_name, 'scd type 1' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( :p_scd1 ), ',' ) AS split_ot )) s
                               ON atc.column_name = s.COLUMN_VALUE
                        UNION
                        SELECT owner, table_name, column_name, 'scd type 2' column_type
                          FROM all_tab_columns atc JOIN TABLE( CAST( td_core.SPLIT( UPPER( :p_scd2 ), ',' ) AS split_ot )) s
                               ON atc.column_name = s.COLUMN_VALUE
                               )
                 WHERE owner = UPPER( :p_owner ) AND table_name = UPPER( :p_table )) s
         ON (t.owner = s.owner AND t.table_name = s.table_name AND t.column_name = s.column_name )
         WHEN MATCHED THEN
            UPDATE
               SET t.column_type = s.column_type, t.modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                   t.modified_dt = SYSDATE
               WHERE s.column_type <> t.column_type
         WHEN NOT MATCHED THEN
            INSERT( t.owner, t.table_name, t.column_name, t.column_type )
            VALUES( s.owner, s.table_name, s.column_name, s.column_type );

      -- do the second merge to write any columns that have been left off
      MERGE INTO column_conf t
         USING ( SELECT owner, table_name, column_name,
                        CASE default_scd_type
                           WHEN 1
                              THEN 'scd type 1'
                           ELSE 'scd type 2'
                        END column_type
                  FROM all_tab_columns JOIN dimension_conf USING( owner, table_name )
                 WHERE owner = UPPER( :p_owner ) AND table_name = UPPER( :p_table )) s
         ON (t.owner = s.owner AND t.table_name = s.table_name AND t.column_name = s.column_name )
         WHEN NOT MATCHED THEN
            INSERT( t.owner, t.table_name, t.column_name, t.column_type )
            VALUES( s.owner, s.table_name, s.column_name, s.column_type );
