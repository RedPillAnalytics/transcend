DROP TABLE error_conf purge
/

CREATE TABLE error_conf
       ( code NUMBER NOT NULL,
	 name VARCHAR2(30) NOT NULL,
	 message VARCHAR2(1000) NOT NULL,
	 created_user	     VARCHAR2(30) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user	     VARCHAR2(30),
	 modified_dt	     DATE
       ) 
/

ALTER TABLE error_conf ADD (
  CONSTRAINT error_conf_pk
 PRIMARY KEY
 (name)
    USING INDEX)
/

ALTER TABLE error_conf ADD (
  CONSTRAINT error_conf_uk1
 unique
 (code)
    USING INDEX)
/


ALTER TABLE error_conf ADD CONSTRAINT error_conf_ck1 CHECK (name=lower(name));

INSERT INTO error_conf (code,name,message) VALUES (-20101, 'unrecognized_parm','The specified parameter value is not recognized');
INSERT INTO error_conf (code,name,message) VALUES (-20102, 'file_too_small','File size smaller than MAX_BYTES paramter');
INSERT INTO error_conf (code,name,message) VALUES (-20103, 'notify_method_invalid','The notification method is not valid');
INSERT INTO error_conf (code,name,message) VALUES (-20104, 'no_files_found','No files found for this configuration');
INSERT INTO error_conf (code,name,message) VALUES (-20105, 'no_tab','The specified table does not exist');
INSERT INTO error_conf (code,name,message) VALUES (-20106, 'no_ext_files','There are no files found for this external table');
INSERT INTO error_conf (code,name,message) VALUES (-20107, 'reject_limit_exceeded','The external table reject limit was exceeded');
INSERT INTO error_conf (code,name,message) VALUES (-20108, 'ext_file_missing','The physical file specified by the external table LOCATION does not exist');
INSERT INTO error_conf (code,name,message) VALUES (-20109, 'on_clause_missing','The ON clause of the MERGE statement was invalid'||chr(10)||'If P_COLUMNS is not provided, then check to see that a primary or unique key exists');
INSERT INTO error_conf (code,name,message) VALUES (-20110, 'incorrect_parameters','The combination of parameters provided yields no matching objects.');
INSERT INTO error_conf (code,name,message) VALUES (-20111, 'no_object','The specified object does not exist');
INSERT INTO error_conf (code,name,message) VALUES (-20112, 'file_too_big','File size larger than MAX_BYTES paramter');
INSERT INTO error_conf (code,name,message) VALUES (-20113, 'not_partitioned','The specified table is not partititoned');
INSERT INTO error_conf (code,name,message) VALUES (-20114, 'parms_not_compatible','The specified parameters are not compatible');
INSERT INTO error_conf (code,name,message) VALUES (-20115, 'parm_not_configured','The specified parameter is not configured');
INSERT INTO error_conf (code,name,message) VALUES (-20116, 'file_not_found','Expected file does not exist');
INSERT INTO error_conf (code,name,message) VALUES (-20117, 'not_iot','The specified table is not index-organized');
INSERT INTO error_conf (code,name,message) VALUES (-20118, 'not_compressed','The specified segment is not compresed');
INSERT INTO error_conf (code,name,message) VALUES (-20119, 'no_part','The specified partition does not exist');
INSERT INTO error_conf (code,name,message) VALUES (-20120, 'partitioned','The specified table is partitioned');
INSERT INTO error_conf (code,name,message) VALUES (-20121, 'iot','The specified table is index-organized');
INSERT INTO error_conf (code,name,message) VALUES (-20122, 'compressed','The specified segment is compresed');
INSERT INTO error_conf (code,name,message) VALUES (-20123, 'no_stats','The specified segment has no stored statistics');
INSERT INTO error_conf (code,name,message) VALUES (-20124, 'no_or_wrong_object','The specified object does not exist or is of the wrong type');
INSERT INTO error_conf (code,name,message) VALUES (-20125, 'too_many_objects','The specified parameters yield more than one object');
INSERT INTO error_conf (code,name,message) VALUES (-20126, 'owb_flow_err','An error was returned from the OWB Control Center');
INSERT INTO error_conf (code,name,message) VALUES (-20127, 'parm_not_supported','The specified parameter is not supported');
