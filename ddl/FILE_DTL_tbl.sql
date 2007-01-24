DROP TABLE efw.file_dtl purge
/

CREATE TABLE efw.file_dtl
       ( filename VARCHAR2(100) NOT NULL,
	 filenumber NUMBER NOT NULL,
	 archfilename VARCHAR2(100) NOT NULL,
	 jobname VARCHAR2(50) NOT NULL,
	 jobnumber NUMBER NOT NULL,
	 num_bytes NUMBER,
	 num_lines NUMBER,
	 file_dt DATE,
	 processed_ts TIMESTAMP,
	 ext_tab_ind VARCHAR2(1),
	 ext_filename VARCHAR2(50),
	 alt_ext_tab_ind VARCHAR2(1),
	 session_id number)
       TABLESPACE efw
/

COMMENT ON TABLE efw.file_dtl IS 'Detail information about each file that is processed by File Mover.'
/

COMMENT on COLUMN EFW.FILE_DTL.FILENAME is 'Name of the source file.';
COMMENT on COLUMN EFW.FILE_DTL.ARCHFILENAME is 'Name of the archived file.';
COMMENT on COLUMN EFW.FILE_DTL.JOBNAME is 'Name of the job that defined in FILE_CTL.';
COMMENT on COLUMN EFW.FILE_DTL.JOBNUMBER is 'Number of the job defined in FILE_CTL.';
COMMENT on COLUMN EFW.FILE_DTL.NUM_BYTES is 'File size of the source file.';
COMMENT on COLUMN EFW.FILE_DTL.NUM_LINES is 'Number of lines in the file.';
COMMENT on COLUMN EFW.FILE_DTL.FILE_DT is 'Timestamp of the file from the filesystem.';
COMMENT on COLUMN EFW.FILE_DTL.PROCESSED_TS is 'Time the file was processed by File Mover.';
COMMENT on COLUMN EFW.FILE_DTL.EXT_TAB_IND is 'Whether or not the file was copied to an external table directory.';
COMMENT on COLUMN EFW.FILE_DTL.EXT_FILENAME is 'Name file was copied to for use in external table.';
COMMENT on COLUMN EFW.FILE_DTL.ALT_EXT_TAB_IND is 'Whether or not the external table needs to be processed.';
COMMENT on COLUMN EFW.FILE_DTL.SESSION_ID is 'SESSION_ID of the processing session.';

GRANT SELECT ON EFW.FILE_DTL TO efw_filemover;
/

CREATE SEQUENCE efw.file_dtl_seq
/