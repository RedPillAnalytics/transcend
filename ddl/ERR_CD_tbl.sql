DROP TABLE err_cd purge
/

CREATE TABLE err_cd
       ( code NUMBER NOT NULL,
	 NAME VARCHAR2(30) NOT NULL,
	 message VARCHAR2(1000) NOT NULL,
	 modified_user     VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	 modified_dt       DATE DEFAULT SYSDATE NOT NULL
       ) 
/

ALTER TABLE err_cd ADD (
  CONSTRAINT err_cd_pk
 PRIMARY KEY
 (code)
    USING INDEX)
/

ALTER TABLE err_cd ADD (
  CONSTRAINT err_cd_uk1
 unique
 (name)
    USING INDEX)
/

INSERT INTO err_cd (code,name,message) VALUES (-20010, 'unrecognized_parm','The specified parameter value is not recognized');
INSERT INTO err_cd (code,name,message) VALUES (-20011, 'file_too_small','File size smaller than MAX_BYTES paramter');
INSERT INTO err_cd (code,name,message) VALUES (-20012, 'notify_method_invalid','The notification method is not valid');
INSERT INTO err_cd (code,name,message) VALUES (-20013, 'no_files_found','No files found for this configuration');
INSERT INTO err_cd (code,name,message) VALUES (-20015, 'no_tab','The specified table does not exist');
INSERT INTO err_cd (code,name,message) VALUES (-20016, 'no_ext_files','There are no files found for this external table');
INSERT INTO err_cd (code,name,message) VALUES (-20017, 'reject_limit_exceeded','The external table reject limit was exceeded');
INSERT INTO err_cd (code,name,message) VALUES (-20018, 'ext_file_missing','The physical file specified by the external table LOCATION does not exist');
INSERT INTO err_cd (code,name,message) VALUES (-20019, 'on_clause_missing','The ON clause of the MERGE statement was invalid'||chr(10)||'If P_COLUMNS is not provided, then check to see that a primary or unique key exists');
INSERT INTO err_cd (code,name,message) VALUES (-20020, 'incorrect_parameters','The combination of parameters provided yields no matching objects.');
INSERT INTO err_cd (code,name,message) VALUES (-20021, 'no_object','The specified object does not exist');
INSERT INTO err_cd (code,name,message) VALUES (-20022, 'file_too_big','File size larger than MAX_BYTES paramter');
INSERT INTO err_cd (code,name,message) VALUES (-20023, 'not_partitioned','The specified table is not partititoned');
INSERT INTO err_cd (code,name,message) VALUES (-20024, 'parms_not_compatible','The specified parameters are not compatible');
INSERT INTO err_cd (code,name,message) VALUES (-20025, 'parm_not_configured','The specified parameter is not configured');
INSERT INTO err_cd (code,name,message) VALUES (-20026, 'file_not_found','Expected file does not exist');
INSERT INTO err_cd (code,name,message) VALUES (-20027, 'no_session_parm','The specified parameter name is not a recognized database parameter');
INSERT INTO err_cd (code,name,message) VALUES (-20028, 'not_iot','The specified table is not index-organized');
INSERT INTO err_cd (code,name,message) VALUES (-20029, 'not_compressed','The specified segment is not compresed');
INSERT INTO err_cd (code,name,message) VALUES (-20030, 'no_part','The specified partition does not exist');
INSERT INTO err_cd (code,name,message) VALUES (-20031, 'partitioned','The specified table is partitioned');
INSERT INTO err_cd (code,name,message) VALUES (-20032, 'iot','The specified table is index-organized');
INSERT INTO err_cd (code,name,message) VALUES (-20033, 'compressed','The specified segment is compresed');
INSERT INTO err_cd (code,name,message) VALUES (-20034, 'no_stats','The specified segment has no stored statistics');
INSERT INTO err_cd (code,name,message) VALUES (-20035, 'no_or_wrong_object','The specified object does not exist or is of the wrong type');
INSERT INTO err_cd (code,name,message) VALUES (-20036, 'too_many_objects','The specified parameters yield more than one object');
INSERT INTO err_cd (code,name,message) VALUES (-20037, 'no_priority','The specified accessor has no priority configured for it');
