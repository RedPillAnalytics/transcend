DROP TABLE files_detail CASCADE CONSTRAINTS purge
/

DROP SEQUENCE files_detail_seq
/

CREATE TABLE files_detail
       ( file_detail_id NUMBER NOT NULL,
	 file_label VARCHAR2(50),
	 file_group VARCHAR2(64),
	 file_type VARCHAR2(7) NOT null,
	 source_filepath VARCHAR2(200),
	 target_filepath VARCHAR2(200),
	 arch_filepath VARCHAR2(100) NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 session_id NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT null)
/

ALTER TABLE files_detail ADD (
  CONSTRAINT file_detail_pk
 PRIMARY KEY
 (file_detail_id)
    USING INDEX)
/

ALTER TABLE files_detail
      ADD (
	    CONSTRAINT file_detail_fk1
	    FOREIGN KEY ( file_label, file_group )
	    REFERENCES files_conf
	    ( file_label, file_group )
	  )
/


COMMENT ON TABLE files_detail IS 'detail information about each file that is processed by the File Package'
/

COMMENT ON COLUMN files_detail.file_detail_id IS 'sequence generated number as primary key';
COMMENT ON COLUMN files_detail.file_label IS 'from the FILES_CONF table';
COMMENT ON COLUMN files_detail.file_group IS 'from the FILES_CONF table';
COMMENT ON COLUMN files_detail.file_type IS 'from the FILES_CONF table';
COMMENT ON COLUMN files_detail.source_filepath IS 'absolute path of the source file (applicable for feeds only)';
COMMENT ON COLUMN files_detail.target_filepath IS 'absolute path of the target file... external table location for feeds and extract file name for extracts.';
COMMENT ON COLUMN files_detail.arch_filepath IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN files_detail.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN files_detail.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN files_detail.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN files_detail.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN files_detail.session_id IS 'AUDSID number of the oracle session';

CREATE SEQUENCE files_detail_seq
/
