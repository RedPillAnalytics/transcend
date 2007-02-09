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
	 src_filename VARCHAR2(200),
	 trg_filename VARCHAR2(200),
	 arch_filename VARCHAR2(100) NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 object_completion VARCHAR2(10),
	 notification VARCHAR2(20),
	 notification_id number,
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
COMMENT ON COLUMN tdinc.filehub_detail.src_filename IS 'absolute path of the source file (applicable for feeds only)';
COMMENT ON COLUMN tdinc.filehub_detail.trg_filename IS 'absolute path of the target file... external table location for feeds and extract file name for extracts.';
COMMENT ON COLUMN tdinc.filehub_detail.arch_filename IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN tdinc.filehub_detail.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN tdinc.filehub_detail.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN tdinc.filehub_detail.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN tdinc.filehub_detail.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN tdinc.filehub_detail.object_completion IS 'Holds logging information for further processing. Can be null, "external" or "external alter" to specify what level of external table processing was done.';
COMMENT ON COLUMN tdinc.filehub_detail.notification IS 'Logging messages concerning the notification piece';
COMMENT ON COLUMN tdinc.filehub_detail.notification_id IS 'identifier from the NOTIFICATION table';
COMMENT ON COLUMN tdinc.filehub_detail.session_id IS 'AUDSID number of the oracle session';

GRANT SELECT ON TDINC.FILEHUB_DETAIL TO tdinc_filehub;
/

CREATE SEQUENCE tdinc.filehub_detail_seq
/