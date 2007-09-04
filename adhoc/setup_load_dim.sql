TRUNCATE TABLE dimension_conf;
TRUNCATE TABLE column_conf;
TRUNCATE TABLE index_maint_conf;
TRUNCATE TABLE constraint_maint_conf;

INSERT INTO dimension_conf(owner,table_name,source_owner,source_object,replace_method,sequence_owner,sequence_name) 
       VALUES ('stewart','test_dim','stewart','test_stg','rename','stewart','test_dim_seq');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','test_key','surrogate key');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','birthdate','scd type 1');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','name','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','zip','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','zip_plus4','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','nat_key','natural key');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','effect_start_dt','effective start date');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','effect_end_dt','effective end date');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('stewart','test_dim','current_ind','current indicator');
