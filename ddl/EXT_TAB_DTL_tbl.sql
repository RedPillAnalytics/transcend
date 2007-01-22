DROP TABLE common.ext_tab_dtl purge
/

CREATE TABLE common.ext_tab_dtl
       ( ext_table VARCHAR2(100) NOT NULL,
	 ext_tab_owner VARCHAR2(100) NOT NULL,
	 jobname VARCHAR2(50) NOT NULL,
	 jobnumber NUMBER NOT NULL,
	 processed_ts TIMESTAMP NOT NULL,
	 num_rows NUMBER,
	 num_lines NUMBER,
	 reject_pcnt NUMBER,
	 session_id NUMBER NOT NULL)
       TABLESPACE common_data
/

COMMENT ON TABLE common.ext_tab_dtl IS 'Detail information about external tables after File Mover runs.'
/

COMMENT ON COLUMN common.ext_tab_dtl.ext_table IS 'Name of the external table.';
COMMENT ON COLUMN common.ext_tab_dtl.ext_tab_owner IS 'Owner of the external table.';
COMMENT ON COLUMN common.ext_tab_dtl.jobname IS 'Name of the job that defined in FILE_CTL.';
COMMENT ON COLUMN common.ext_tab_dtl.jobnumber IS 'Number of the job defined in FILE_CTL.';
COMMENT ON COLUMN common.ext_tab_dtl.processed_ts IS 'Time the file was processed by File Mover.';
COMMENT ON COLUMN common.ext_tab_dtl.num_rows IS 'Result of a "SELECT COUNT(*)..." on the table.';
COMMENT ON COLUMN common.ext_tab_dtl.num_lines IS 'The number of lines at the OS level in the file or files that make up the external table.';
COMMENT ON COLUMN common.ext_tab_dtl.reject_pcnt IS 'Percentage of records rejected';
COMMENT ON COLUMN common.ext_tab_dtl.session_id IS 'SESSION_ID of the processing session.';

GRANT SELECT ON common.ext_tab_dtl TO file_mover
/
