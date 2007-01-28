DROP TABLE efw.filehub_detail CASCADE CONSTRAINTS purge
/

DROP SEQUENCE efw.filehub_detail_seq
/

CREATE TABLE efw.filehub_detail
       ( fh_detail_id NUMBER NOT NULL,
	 filehub_id NUMBER NOT NULL,
	 filehub_name VARCHAR2(50),
	 filehub_group VARCHAR2(64),
	 filehub_type VARCHAR2(7) NOT null,
	 src_filename VARCHAR2(200),
	 trg_filename VARCHAR2(200),
	 arch_filename VARCHAR2(100) NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 ext_tab_process VARCHAR2(5),
	 session_id NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT null)
       TABLESPACE efw
/

COMMENT ON TABLE efw.filehub_detail IS 'detail information about each file that is processed by the File Package'
/

COMMENT ON COLUMN efw.filehub_detail.fh_detail_id IS 'sequence generated number as primary key';
COMMENT ON COLUMN efw.filehub_detail.filehub_id IS 'parent key from the FILEHUB_CONF table.';
COMMENT ON COLUMN efw.filehub_detail.filehub_name IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN efw.filehub_detail.filehub_group IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN efw.filehub_detail.filehub_type IS 'from the FILEHUB_CONF table';
COMMENT ON COLUMN efw.filehub_detail.src_filename IS 'absolute path of the source file (applicable for feeds only)';
COMMENT ON COLUMN efw.filehub_detail.trg_filename IS 'absolute path of the target file... external table location for feeds and extract file name for extracts.';
COMMENT ON COLUMN efw.filehub_detail.arch_filename IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN efw.filehub_detail.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN efw.filehub_detail.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN efw.filehub_detail.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN efw.filehub_detail.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN efw.filehub_detail.ext_tab_process IS '"move" means that this file will be part of an external table; "alter" means the same, but, the table will be altered to include the files for this run, or "none" meaning there is no external table associated.';
COMMENT ON COLUMN efw.filehub_detail.session_id IS 'AUDSID number of the oracle session';

GRANT SELECT ON EFW.FILEHUB_DETAIL TO efw_filehub;
/

CREATE SEQUENCE efw.filehub_detail_seq
/