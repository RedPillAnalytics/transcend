DROP TABLE efw.filehub_dtl CASCADE CONSTRAINTS purge
/

DROP SEQUENCE efw.filehub_dtl_seq
/

CREATE TABLE efw.filehub_dtl
       ( filehub_dtl_id NUMBER NOT NULL,
	 src_filename VARCHAR2(200),
	 trg_filename VARCHAR2(200),
	 arch_filename VARCHAR2(100) NOT NULL,
	 filehub_type VARCHAR2(7) NOT null,
	 jobname VARCHAR2(50) NOT NULL,
	 filehub_id NUMBER NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP NOT NULL,
	 ext_tab_process VARCHAR2(1),
	 session_id NUMBER NOT null)
       TABLESPACE efw
/

COMMENT ON TABLE efw.filehub_dtl IS 'detail information about each file that is processed by the File Package'
/

COMMENT ON COLUMN efw.filehub_dtl.filehub_dtl_id IS 'sequence generated number as primary key';
COMMENT ON COLUMN efw.filehub_dtl.src_filename IS 'absolute path of the source file (applicable for feeds only)';
COMMENT ON COLUMN efw.filehub_dtl.trg_filename IS 'absolute path of the target file... external table location for feeds and extract file name for extracts.';
COMMENT ON COLUMN efw.filehub_dtl.filehub_type IS 'defines whether the process was a feed or an extract.';
COMMENT ON COLUMN efw.filehub_dtl.arch_filename IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN efw.filehub_dtl.jobname IS 'job that called the File Package';
COMMENT ON COLUMN efw.filehub_dtl.filehub_id IS 'primary key of the FILEHUB_CONF table';
COMMENT ON COLUMN efw.filehub_dtl.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN efw.filehub_dtl.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN efw.filehub_dtl.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN efw.filehub_dtl.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN efw.filehub_dtl.ext_tab_ind IS 'Y/N indicator of whether this file will used as an external table';
COMMENT ON COLUMN efw.filehub_dtl.alt_ext_tab_ind IS 'Y/N indicator of whether the external table containing this file should be altered to include';
COMMENT ON COLUMN efw.filehub_dtl.session_id IS 'AUDSID number of the oracle session';

GRANT SELECT ON EFW.FILEHUB_DTL TO efw_file;
/

CREATE SEQUENCE efw.filehub_dtl_seq
/