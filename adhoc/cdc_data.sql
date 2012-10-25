
-- start
EXEC trans_adm.delete_cdc_source('ORCL Source');

begin
   trans_adm.create_cdc_source
   ( p_source_name      => 'ORCL Source',
     p_source_type      => 'goldengate',
     p_service_name     => 'orcl',
     p_hostname         => 'localhost',
     p_port             => 1521,
     p_dblink           => 'orcl.loopback',
     p_ogg_group_key    => 1,
     p_ogg_group_name   => 'test',
     p_ogg_check_table  => 'goldengate.oggckpt',
     p_ogg_check_column => 'log_cmplt_csn'
   );
END;
/

INSERT INTO tdrep.cdc_group
       (
         source_name,
         group_name,
         subscription,
         filter_policy,
         interface,
         interface_prefix
       )
       VALUES
       (
         'ORCL Source',
         'test',
         'stewfnd',
         'interface',
         'stewfnd',
         'c$'
       );


INSERT INTO tdrep.cdc_entity
       (
         source_table,
         source_owner,
         group_name,
         natkey_list
       )
       VALUES
       (
         'test',
         'stewart',
         'test',
         'test_id'
       );

INSERT INTO tdrep.cdc_subscription
       (
         sub_name,
         group_name,
         effective_scn,
         expiration_scn
       )
       VALUES
       (
         'test1',
         'test',
         0,
         5
       );

INSERT INTO tdrep.cdc_subscription
       (
         sub_name,
         group_id,
         effective_scn,
         expiration_scn
       )
       VALUES
       (
         'test2',
         'test',
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
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'oracle_scn',
         'source_scn',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'rowsid',
         'row_rank',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'dml_type',
         'dml_type',
         'varchar2(30)'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'commit_date',
         'commit_date',
         'date'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'effect_scn',
         'source_minscn',
         'number'
       );

INSERT INTO tdrep.cdc_audit_datatype
       ( 
         group_name,
         column_name,
         column_type,
         datatype
       )
       VALUES
       (
         'test',
         'expire_scn',
         'source_maxscn',
         'number'
       );
