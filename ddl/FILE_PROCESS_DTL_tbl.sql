DROP TABLE efw.file_process_dtl CASCADE CONSTRAINTS purge
/

DROP SEQUENCE efw.file_process_dtl_seq
/

CREATE TABLE efw.file_process_dtl
       ( filenumber NUMBER NOT NULL,
	 filename VARCHAR2(50) NOT null,
	 file_process_type VARCHAR2(1) NOT null,
	 arch_filename VARCHAR2(100) NOT NULL,
	 jobname VARCHAR2(50) NOT NULL,
	 file_process_id NUMBER NOT NULL,
	 num_bytes NUMBER NOT null,
	 num_lines NUMBER NOT null,
	 file_dt DATE NOT null,
	 processed_ts TIMESTAMP NOT null
	 ext_filename VARCHAR2(100),
	 ext_tab_ind VARCHAR2(1),
	 alt_ext_tab_ind VARCHAR2(1),
	 session_id NUMBER NOT null)
       TABLESPACE efw
/

COMMENT ON TABLE efw.file_process_dtl IS 'detail information about each file that is processed by the File Package'
/

COMMENT ON COLUMN efw.file_process_dtl.filenumber IS 'sequence generated number as primary key';
COMMENT ON COLUMN efw.file_process_dtl.filename IS 'absolute path of the file being audited';
COMMENT ON COLUMN efw.file_process_dtl.arch_filename IS 'absolute path of the archived version of the file';
COMMENT ON COLUMN efw.file_process_dtl.jobname IS 'job that called the File Package';
COMMENT ON COLUMN efw.file_process_dtl.file_process_id IS 'primary key of the FILE_PROCESS_CONF table';
COMMENT ON COLUMN efw.file_process_dtl.num_bytes IS 'size in bytes of the file';
COMMENT ON COLUMN efw.file_process_dtl.num_lines IS 'number of lines in the file';
COMMENT ON COLUMN efw.file_process_dtl.file_dt IS 'last modified date on the file';
COMMENT ON COLUMN efw.file_process_dtl.processed_ts IS 'date the file was processed by File Package';
COMMENT ON COLUMN efw.file_process_dtl.src_filename IS 'the external table location (applicable for feeds only)';
COMMENT ON COLUMN efw.file_process_dtl.ext_tab_ind IS 'Y/N indicator of whether this file will used as an external table';
COMMENT ON COLUMN efw.file_process_dtl.alt_ext_tab_ind IS 'Y/N indicator of whether the external table containing this file should be altered to include';
COMMENT ON COLUMN efw.file_process_dtl.session_id IS 'AUD_SID number of the oracle session';

GRANT SELECT ON EFW.FILE_PROCESS_DTL TO efw_file;
/

CREATE SEQUENCE efw.file_process_dtl_seq
/