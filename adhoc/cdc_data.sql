
DELETE tdrep.cdc_source;

ALTER SEQUENCE cdc_source_seq nextvalue 1;

ALTER SEQUENCE cdc_group_seq nextvalue 1;

ALTER SEQUENCE cdc_subscription_seq nextvalue 1;

EXEC trans_adm.create_cdc_source
     ( p_source_type            => 'goldengate',
       p_service_name           => 'orcl',
       p_hostname               => 'localhost',
       p_port                   => 1521,
       p_dblink_name            => 'orcl.loopback',
       p_ogg_group_key          => 1,
       p_ogg_group_name         => 'test',
       p_ogg_check_table        => 'goldengate.oggckpt',
       p_ogg_check_column       => 'log_cmplt_scn'
     );

EXEC trans_adm.create_cdc_group
     ( p_group_name             => 'demo',
       p_source_id              => 1,
       p_foundation             => 'stewfnd',
       p_subscription           => 'stewfnd',
       p_filter_policy          => 'subscription',
       p_sub_prefix             => 'c$'
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

