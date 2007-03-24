DROP TABLE tdinc.filehub_detail CASCADE CONSTRAINTS purge
/

DROP SEQUENCE tdinc.filehub_detail_seq
/

CREATE TABLE tdinc.filehub_detail
       ( fh_detail_id NUMBER NOT NULL,
	 filehub_id NUMBER NOT NULL,
	 filehub_name VARCHAR2(50),
	 filehub_group VARCHAR2(64),
	 filehub_type VARCHAR2(7) NOT null,
	 source_filepath VARCHAR2(200),
	 target_filepath VARCHAR2(200),
	 arch_filepath VARCHAR2(100) NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 session_id NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT null)
       TABLESPACE tdinc
/

COMMENT ON TABLE tdinc.filehub_detail IS 'detail information about each file that is processed by the File Package'
/

COMMENT ON COLUMN tdinc.filehub_detail.fh_detail_id IS 'sequence generated number as primary key';
COMMENT ON COLUMN tdinc.filehub_detail.filehub_id IS 'parent key from the FILEHUB_CONF table.';
COMMENT ON COLUMN tdinc.filehub_detail.filehub_name IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN tdinc.filehub_detail.filehub_group IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN tdinc.filehub_detail.filehub_type IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN tdinc.filehub_detail.source_filepath IS 'absolute path of the source file (applicable for feeds only)';
COMMENT ON COLUMN tdinc.filehub_detail.target_filepath IS 'absolute path of the target file... external table location for feeds and extract file name for extracts.';
COMMENT ON COLUMN tdinc.filehub_detail.arch_filepath IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN tdinc.filehub_detail.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN tdinc.filehub_detail.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN tdinc.filehub_detail.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN tdinc.filehub_detail.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN tdinc.filehub_detail.session_id IS 'AUDSID number of the oracle session';

GRANT SELECT ON TDINC.FILEHUB_DETAIL TO tdinc_filehub;
/

CREATE SEQUENCE tdinc.filehub_detail_seq
/