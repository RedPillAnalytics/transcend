DROP TABLE efw.filehub_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE efw.filehub_conf_seq
/

CREATE TABLE efw.filehub_conf
       ( filehub_id		NUMBER		NOT NULL,
	 filehub_name		VARCHAR2(100) 	NOT NULL,
	 filehub_group		VARCHAR2(64) 	NOT NULL,
	 filehub_type		VARCHAR2(7) 	NOT NULL,
	 object_owner		VARCHAR2(30)    NOT NULL,
	 object_name		VARCHAR2(30)    NOT NULL,
	 trg_filename		VARCHAR2(30) 	NOT NULL,
	 dirname        	VARCHAR2(30)    NOT NULL,
	 arch_dirname     	VARCHAR2(30) 	NOT NULL,
	 secondary_dir	 	VARCHAR2(50)	DEFAULT 'NA' NOT NULL,
	 min_bytes		NUMBER 		DEFAULT 0 NOT NULL,
	 max_bytes              NUMBER 		DEFAULT 0 NOT NULL,
	 file_timestamp		VARCHAR2(30) 	DEFAULT 'yyyymmddhhmissxff' NOT NULL,
	 file_notification	VARCHAR2(6) 	DEFAULT 'none' NOT NULL,
	 source_dir	 	VARCHAR2(50) 	NOT NULL,
	 source_regexp   	VARCHAR2(100) 	NOT NULL,
	 regexp_options		VARCHAR2(10)    DEFAULT 'i' NOT NULL,
	 multi_files_action	VARCHAR2(10) 	DEFAULT 'newest' NOT null,
	 file_requirement	VARCHAR2(8) 	DEFAULT 'required' NOT null,
	 dateformat		VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss am' NOT NULL,
	 delimiter		VARCHAR2(1)    	DEFAULT ',' NOT NULL,
	 quotechar		VARCHAR2(1) 	DEFAULT '"' NOT NULL,
	 include_headers	VARCHAR2(7) 	DEFAULT 'headers' NOT NULL,
	 created_user   	VARCHAR2(30) 	DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL,
	 created_dt     	DATE 		DEFAULT sysdate,
	 modified_user  	VARCHAR2(30)	DEFAULT sys_context('USERENV','SESSION_USER'),
	 modified_dt    	DATE		DEFAULT sysdate
       )
       TABLESPACE efw
/

COMMENT ON TABLE efw.filehub_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN efw.filehub_conf.filehub_id IS 'sequence generated primary key of the table';
COMMENT ON COLUMN efw.filehub_conf.filehub_name IS 'unique name for each distinct process';
COMMENT ON COLUMN efw.filehub_conf.filehub_group IS 'logical grouping of filehub_ids that can be called together, such as files that are loaded in the same job';
COMMENT ON COLUMN efw.filehub_conf.filehub_type IS 'type of filehub, "feed" or "extract"';
COMMENT ON COLUMN efw.filehub_conf.trg_filename IS 'expected filename for the target of the process';
COMMENT ON COLUMN efw.filehub_conf.object_owner IS 'owner of the schema object associated with the file';
COMMENT ON COLUMN efw.filehub_conf.object_name IS 'name of the schema object associated with the file';
COMMENT ON COLUMN efw.filehub_conf.dirname IS 'name of the oracle directory object for the file';
COMMENT ON COLUMN efw.filehub_conf.arch_dirname IS 'name of the oracle directory object for an archive of the file';
COMMENT ON COLUMN efw.filehub_conf.min_bytes IS 'minimum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.filehub_conf.max_bytes IS 'maximum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN efw.filehub_conf.file_timestamp IS 'NLS_TIMESTAMP_FORMAT to use for the timestamp written on the file. A value of NA means that no timestamp will be written on the file.'; 
COMMENT ON COLUMN efw.filehub_conf.created_user IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.modified_dt IS 'for auditing';
COMMENT ON COLUMN efw.filehub_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN efw.filehub_conf.regexp_options IS 'additional match options that can specified in the regular expression';
COMMENT ON COLUMN efw.filehub_conf.source_dir IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN efw.filehub_conf.secondary_dir IS 'A directory to write an exact copy of each file to, in case that functionality is needed';
COMMENT ON COLUMN efw.filehub_conf.multi_files_action IS 'Action to take is multiple files match SOURCE_REGEXP. Current options are "newest","oldest","all","fail" or "proceed"';
COMMENT ON COLUMN efw.filehub_conf.file_requirement IS '"required" or "none": determines whether the job fails or not when files are not found.';
COMMENT ON COLUMN efw.filehub_conf.dateformat IS 'NLS_DATE_FORMAT of date columns in the extract';
COMMENT ON COLUMN efw.filehub_conf.delimiter IS 'delimiter used to separate columns';
COMMENT ON COLUMN efw.filehub_conf.quotechar IS 'quotechar used to support columns. An NA specifies that no quotechar is used';
COMMENT ON COLUMN efw.filehub_conf.include_headers IS 'a indicator of whether headers should be included as the first row in the file: "headers" or "none"';


ALTER TABLE efw.filehub_conf ADD (
  CONSTRAINT filehub_conf_pk
 PRIMARY KEY
 (filehub_id)
    USING INDEX
    TABLESPACE efw)
/

ALTER TABLE efw.filehub_conf
      ADD CONSTRAINT filehub_conf_uk1 UNIQUE (filehub_name,filehub_group)
      USING INDEX TABLESPACE efw
/

CREATE SEQUENCE efw.filehub_conf_seq
/
