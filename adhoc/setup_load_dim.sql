exec td_dbapi.disable_constraints('tdrep','dimension_conf',p_basis=>'reference');
TRUNCATE TABLE tdrep.column_conf;
TRUNCATE TABLE tdrep.dimension_conf;
exec td_dbapi.enable_constraints('tdrep','dimension_conf',p_basis=>'reference');

INSERT INTO tdrep.dimension_conf(owner,table_name,source_owner,source_object,replace_method,sequence_owner,sequence_name) 
       VALUES ('stewart','test_dim','stewart','test_stg','rename','stewart','test_dim_seq');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','test_key','surrogate key');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','birthdate','scd type 1');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','name','scd type 2');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','zip','scd type 2');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','zip_plus4','scd type 2');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','nat_key','natural key');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','effect_start_dt','effective start date');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','effect_end_dt','effective end date');
INSERT INTO tdrep.column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','current_ind','current indicator');