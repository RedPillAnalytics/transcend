DROP TABLE tdinc.err_cd
/

CREATE TABLE tdinc.err_cd
       ( code NUMBER NOT NULL,
	 NAME VARCHAR2(30) NOT NULL,
	 message VARCHAR2(1000) NOT NULL,
	 modified_user     VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	 modified_dt       DATE DEFAULT SYSDATE NOT NULL
       ) 
TABLESPACE tdinc
/

ALTER TABLE tdinc.err_cd ADD (
  CONSTRAINT err_cd_pk
 PRIMARY KEY
 (code)
    USING INDEX
    TABLESPACE tdinc)
/

ALTER TABLE tdinc.err_cd ADD (
  CONSTRAINT err_cd_uk1
 unique
 (name)
    USING INDEX
    TABLESPACE tdinc)
/

INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20010, 'file_too_big','File size larger than MAX_BYTES paramter');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20011, 'file_too_small','File size smaller than MAX_BYTES paramter');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20012, 'notify_method_invalid','The notification method is not valid');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20013, 'no_files_found','No files found for this configuration');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20015, 'no_ext_tab','External table in this configuration is non-existent');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20016, 'no_ext_files','There are no files found for this external table');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20017, 'reject_limit_exceeded','The external table reject limit was exceeded');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20018, 'ext_file_missing','The physical file specified by the external table LOCATION does not exist');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20019, 'on_clause_missing','The ON clause of the MERGE statement was invalid'||chr(10)||'If P_COLUMNS is not provided, then check to see that a primary or unique key exists');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20020, 'incorrect_parameters','The combination of parameters provided yields no matching objects.');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20021, 'no_object','The specified object does not exist');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20022, 'unrecognized_parm','The specified parameter is not recognized');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20023, 'not partitioned','The specified table is not partititoned');