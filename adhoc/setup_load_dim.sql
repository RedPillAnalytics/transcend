DELETE FROM column_conf WHERE owner='STEWART' AND table_name='TEST_DIM';
DELETE FROM dimension_conf WHERE owner='STEWART' AND table_name='TEST_DIM';

INSERT INTO dimension_conf(owner,table_name,source_owner,source_object,sequence_owner,sequence_name) 
       VALUES ('STEWART','TEST_DIM','STEWART','TEST_STG','STEWART','TEST_DIM_SEQ');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','TEST_KEY','surrogate key');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','BIRTHDATE','scd type 1');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','NAME','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','ZIP','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','ZIP_PLUS4','scd type 2');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','NAT_KEY','natural key');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','EFFECT_START_DT','effective date');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','EFFECT_END_DT','expiration date');
INSERT INTO column_conf(owner,table_name,column_name,column_type) VALUES ('STEWART','TEST_DIM','CURRENT_IND','current indicator');