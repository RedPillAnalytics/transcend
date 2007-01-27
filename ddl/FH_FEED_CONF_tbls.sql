DROP TABLE efw.fh_feed_conf purge
/

CREATE TABLE efw.fh_feed_conf
       ( filehub_id	NUMBER NOT NULL,
	 source_regexp   	VARCHAR2(100) NOT NULL,
	 regexp_ci_ind		VARCHAR2(1) NOT NULL,
	 source_dir	 	VARCHAR2(30) NOT NULL,
	 secondary_dir	 	VARCHAR2(50) NOT null,
	 multi_files_action	VARCHAR2(10) NOT null,
	 files_required_ind	VARCHAR2(1) NOT NULL, 
	 created_user    	VARCHAR2(30) NOT null,
	 created_dt      	DATE NOT null,
	 modified_user   	VARCHAR2(30),
	 modified_dt     	DATE
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.fh_feed_conf IS 'Configuration portition of the File Mover process. The table should only be updated through ETLFW.FH_MOVE.REGISTER_JOB_FILES.'
/

COMMENT ON COLUMN efw.fh_feed_conf.filehub_id IS 'the same value as the FILEHUB_CONF; also the primary key';
COMMENT ON COLUMN efw.fh_feed_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN efw.fh_feed_conf.regexp_ci_ind IS 'indicates whether the REGEXP should be case-insensitive.';
COMMENT ON COLUMN efw.fh_feed_conf.source_dir IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN efw.fh_feed_conf.secondary_dir IS 'A directory to write an exact copy of each file to, in case that functionality is needed';
COMMENT ON COLUMN efw.fh_feed_conf.multi_files_action IS 'Action to take is multiple files match SOURCE_REGEXP. Current options are "newest","oldest","all","fail" or "proceed"';
COMMENT ON COLUMN efw.fh_feed_conf.files_required_ind IS 'A value of "Y" means the job will fail if no files are found.';
COMMENT ON COLUMN efw.fh_feed_conf.created_user IS 'for auditing';
COMMENT ON COLUMN efw.fh_feed_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN efw.fh_feed_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN efw.fh_feed_conf.modified_dt IS 'for auditing';


ALTER TABLE efw.fh_feed_conf
      ADD CONSTRAINT fh_ctl_pk PRIMARY KEY (filehub_id)
      USING INDEX TABLESPACE efw
/

ALTER TABLE efw.fh_feed_conf
      ADD CONSTRAINT fh_ctl_uk2 UNIQUE (source_regexp)
      USING INDEX TABLESPACE efw
/

DROP SEQUENCE efw.fh_feed_conf_seq
/

CREATE SEQUENCE efw.fh_feed_conf_seq START WITH 120
/

GRANT SELECT ON efw.fh_feed_conf TO efw_filemover
/
