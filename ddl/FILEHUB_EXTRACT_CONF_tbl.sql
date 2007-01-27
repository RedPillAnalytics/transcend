DROP TABLE efw.file_extract_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE efw.file_extract_conf
       ( file_process_id NUMBER		NOT NULL,
	 dateformat      VARCHAR2(30)   NOT NULL,
	 delimiter       VARCHAR2(5)    NOT NULL,
	 quotechar       VARCHAR2(1) 	NOT NULL,
	 headers         VARCHAR2(1) 	NOT NULL,
	 sendmail        VARCHAR2(1) 	NOT NULL,
	 sender          VARCHAR2(1024) NOT NULL,
	 recipients      VARCHAR2(2000) NOT NULL,
	 baseurl         VARCHAR2(255) 	NOT NULL,
	 created_user    VARCHAR2(30) 	NOT NULL,
	 created_dt      DATE 		NOT NULL,
	 modified_user 	 VARCHAR2(30),
	 modified_dt	 DATE
)
TABLESPACE efw
/

COMMENT ON TABLE efw.file_extract_conf IS 'table holding configuration information for EXTRACTS in the FILE package';

COMMENT ON COLUMN efw.file_extract_conf.file_process_id IS 'sequence generated number that distinctly identifies a file process. Primary key of the table.';
COMMENT ON COLUMN efw.file_extract_conf.dateformat IS 'NLS_DATE_FORMAT of date columns in the extract';
COMMENT ON COLUMN efw.file_extract_conf.delimiter IS 'delimiter used to separate columns';
COMMENT ON COLUMN efw.file_extract_conf.quotechar IS 'quotechar used to support columns. An NA specifies that no quotechar is used';
COMMENT ON COLUMN efw.file_extract_conf.headers IS 'a Y/N indicator of whether headers should be included as the first row in the filw';
COMMENT ON COLUMN efw.file_extract_conf.sendmail IS 'a Y/N indicator of whether to send emails announcing the extracts';
COMMENT ON COLUMN efw.file_extract_conf.sender IS 'sender of file announcement emails';
COMMENT ON COLUMN efw.file_extract_conf.recipients IS 'comma separated list of email addresses that receive file announcement emails';
COMMENT ON COLUMN efw.file_extract_conf.baseurl IS 'baseurl that serves up the extract file';
COMMENT ON COLUMN efw.file_extract_conf.created_user IS 'for auditing';
COMMENT ON COLUMN efw.file_extract_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN efw.file_extract_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN efw.file_extract_conf.modified_dt IS 'for auditing';


ALTER TABLE efw.file_extract_conf ADD (
  CONSTRAINT file_extract_conf_pk
 PRIMARY KEY
 (file_process_id)
    USING INDEX
    TABLESPACE efw)
/