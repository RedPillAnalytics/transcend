DELETE tdrep.cdc_source;

INSERT INTO tdrep.cdc_source
       ( 
         source_id,
         source_type,
         service_name,
         hostname,
         port,
         dblink_name
       )
       VALUES 
       (
         1, 
         'goldengate',
         'orcl',
         'localhost',
         1521,
         'orcl.loopback'
       );

INSERT INTO tdrep.cdc_source_external
       ( 
         source_id,
         ogg_group_key,
         ogg_group_name,
         ogg_check_table,
         ogg_check_column
       )
       VALUES 
       (
         1, 
         1,
         'test',
         'goldengate.oggckpt',
         'log_cmplt_csn'
       );


INSERT INTO tdrep.cdc_group
       (
         group_id,
         group_name,
         source_id,
         foundation,
         filter_policy,
         subscription,
         sub_prefix
       )
       VALUES
       (
         1,
         'test',
         1,
         'stewfnd',
         'subscription',
         'stewfnd',
         'c$'
       );


INSERT INTO tdrep.cdc_entity
       (
         entity_id,
         source_table,
         source_owner,
         group_id,
         natkey_list
       )
       VALUES
       (
         1,
         'test',
         'stewart',
         1,
         'test_id'
       );

INSERT INTO tdrep.cdc_subscription
       (
         sub_id,
         sub_name,
         group_id,
         effective_scn,
         expiration_scn
       )
       VALUES
       (
         1,
         'test1',
         1,
         0,
         5
       );

INSERT INTO tdrep.cdc_subscription
       (
         sub_id,
         sub_name,
         group_id,
         effective_scn,
         expiration_scn
       )
       VALUES
       (
         2,
         'test2',
         1,
         0,
         5
       );

DELETE goldengate.oggckpt;

INSERT INTO goldengate.oggckpt
       ( 
         group_name,
         group_key,
         rba,
         create_ts,
         last_update_ts,
         current_dir,
         log_cmplt_csn
       )
       values
       (
         'test',
         1,
         1,
         SYSDATE,
         SYSDATE,
         '/tmp',
         '10'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'oracle_scn',
         'source_scn',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'rowsid',
         'row_rank',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'dml_type',
         'dml_type',
         'varchar2(30)'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'commit_date',
         'commit_date',
         'date'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'effect_scn',
         'source_minscn',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_id,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         1,
         'expire_scn',
         'source_maxscn',
         'number'
       );

