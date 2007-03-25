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
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20014, 'no_ext_tab','External table in this configuration is non-existent');
INSERT INTO tdinc.err_cd (code,name,message) VALUES (-20014, 'no_ext_files','There are no files found for this external table.');