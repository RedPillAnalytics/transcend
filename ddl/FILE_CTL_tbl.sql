DROP TABLE efw.file_ctl purge
/

CREATE TABLE efw.file_ctl
       ( jobnumber		NUMBER NOT NULL,
         jobname		VARCHAR2(40) NOT NULL,
	 filename 		VARCHAR2(50) NOT NULL,
	 source_regexp   	VARCHAR2(100) NOT NULL,
	 regexp_ci_ind		VARCHAR2(1) NOT NULL,
	 source_dir	 	VARCHAR2(50) NOT NULL,
	 min_bytes		NUMBER NOT null,
	 max_bytes              NUMBER NOT null,
	 arch_dir        	VARCHAR2(50) NOT NULL,
	 add_arch_ts_ind	VARCHAR2(1) NOT NULL,
	 wrk_dir	 	VARCHAR2(50) NOT null,
	 ext_dir		VARCHAR2(50) NOT null,
	 ext_filename    	VARCHAR2(30) NOT null,
	 ext_table		VARCHAR2(30) NOT null,
	 ext_tab_owner		VARCHAR2(30) NOT null,
	 multi_files_action	VARCHAR2(10) NOT null,
	 files_required_ind	VARCHAR2(1) NOT NULL,	 
	 created_user    	VARCHAR2(30) NOT null,
	 created_dt      	DATE NOT null,
	 modified_user   	VARCHAR2(30),
	 modified_dt     	DATE
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.file_ctl IS 'Configuration portition of the File Mover process. The table should only be updated through ETLFW.FILE_MOVE.REGISTER_JOB_FILES.'
/

COMMENT ON COLUMN efw.file_ctl.jobnumber IS 'Sequence generated unique number for each job.';
COMMENT ON COLUMN efw.file_ctl.jobname IS 'Unique jobname for each job that uses File Mover.';
COMMENT ON COLUMN efw.file_ctl.filename IS 'Unique name for each regular expression configured';
COMMENT ON COLUMN efw.file_ctl.source_regexp IS 'Regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN efw.file_ctl.regexp_ci_ind IS 'Indicates whether the REGEXP should be case-insensitive.';
COMMENT ON COLUMN efw.file_ctl.source_dir IS 'Name of the directory object where the files are pulled from.';
COMMENT ON COLUMN efw.file_ctl.min_bytes IS 'Minimum size threshhold for the source file.';
COMMENT ON COLUMN efw.file_ctl.max_bytes IS 'Maximum size threshhold for the source file.';
COMMENT ON COLUMN efw.file_ctl.arch_dir IS 'Name of the directory to archive the source files to.';
COMMENT ON COLUMN efw.file_ctl.add_arch_ts_ind IS 'Whether or not to attach a timestamp to archived files.';
COMMENT ON COLUMN efw.file_ctl.wrk_dir IS 'A directory to write an exact copy of each file to.';
COMMENT ON COLUMN efw.file_ctl.ext_dir IS 'Write the source file to this directory for processing as an external table.';
COMMENT ON COLUMN efw.file_ctl.ext_filename IS 'The name of the file the external table is expecting.';
COMMENT ON COLUMN efw.file_ctl.ext_table IS 'The name of the external table.';
COMMENT ON COLUMN efw.file_ctl.ext_tab_owner IS 'The owner of the external table.';
COMMENT ON COLUMN efw.file_ctl.multi_files_action IS 'Action to take is multiple files match SOURCE_REGEXP.';
COMMENT ON COLUMN efw.file_ctl.files_required_ind IS 'A value of "Y" means the job will fail if no files are found.';
COMMENT ON COLUMN efw.file_ctl.created_user IS 'For Auditing';
COMMENT ON COLUMN efw.file_ctl.created_dt IS 'For Auditing';
COMMENT ON COLUMN efw.file_ctl.modified_user IS 'For Auditing';
COMMENT ON COLUMN efw.file_ctl.modified_dt IS 'For Auditing';


ALTER TABLE efw.file_ctl
      ADD CONSTRAINT file_ctl_pk PRIMARY KEY (jobnumber)
      USING INDEX TABLESPACE efw
/

ALTER TABLE efw.file_ctl
      ADD CONSTRAINT file_ctl_uk1 UNIQUE (jobname,filename)
      USING INDEX TABLESPACE efw
/

ALTER TABLE efw.file_ctl
      ADD CONSTRAINT file_ctl_uk2 UNIQUE (source_regexp)
      USING INDEX TABLESPACE efw
/

DROP SEQUENCE efw.file_ctl_seq
/

CREATE SEQUENCE efw.file_ctl_seq START WITH 120
/

GRANT SELECT ON efw.file_ctl TO efw_filemover
/
