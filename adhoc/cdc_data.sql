
DELETE tdrep.cdc_source;

ALTER SEQUENCE cdc_source_seq nextvalue 1;

EXEC trans_adm.delete_cdc_source('ORCL Source');

ALTER SEQUENCE cdc_group_seq nextvalue 1;

BEGIN
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

ALTER SEQUENCE cdc_subscription_seq nextvalue 1;

BEGIN
   trans_adm.create_cdc_group
   ( p_source_name      => 'ORCL Source',
     p_group_name       => 'test',
     p_subscription     => 'stewfnd',
     p_interface        => 'stewfnd',
     p_filter_policy    => 'interface',
     p_prefix           => 'c$'
   );
END;
/

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
=======
begin
   trans_adm.create_cdc_entity
   ( p_group_name       => 'test',
     p_source_owner     => 'stewart',
     p_source_table     => 'test',
     p_natkey_list      => 'test_id'
   );
END;
/

EXEC trans_adm.create_cdc_group
     ( p_group_name             => 'demo',
       p_source_id              => 1,
       p_foundation             => 'stewfnd',
       p_subscription           => 'stewfnd',
       p_filter_policy          => 'subscription',
       p_sub_prefix             => 'c$'
     );
=======
begin
   trans_adm.create_cdc_subscription
   ( p_group_name       => 'test',
     p_sub_name         => 'test1',
     p_effective_scn    => 0,
     p_expiration_scn   => 5
   );
END;
/

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
=======
begin
   trans_adm.create_cdc_subscription
   ( p_group_name       => 'test',
     p_sub_name         => 'test2',
     p_effective_scn    => 0,
     p_expiration_scn   => 5
   );
END;
/

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
