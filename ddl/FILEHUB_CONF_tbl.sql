DROP TABLE filehub_conf CASCADE CONSTRAINTS purge
/
DROP SEQUENCE filehub_conf_seq
/

CREATE TABLE filehub_conf
       ( filehub_id		NUMBER		NOT NULL,
	 filehub_name		VARCHAR2(100) 	NOT NULL,
	 filehub_group		VARCHAR2(64) 	NOT NULL,
	 filehub_type		VARCHAR2(7) 	NOT NULL,
	 object_owner		VARCHAR2(30)    NOT NULL,
	 object_name		VARCHAR2(30)    NOT NULL,
	 directory		VARCHAR2(30)	NOT NULL,
	 filename		VARCHAR2(50)    NOT NULL,		
	 arch_directory     	VARCHAR2(30) 	NOT NULL,
	 min_bytes		NUMBER 		DEFAULT 0 NOT NULL,
	 max_bytes              NUMBER 		DEFAULT 0 NOT NULL,
	 file_datestamp		VARCHAR2(30),
	 baseurl                VARCHAR2(500),
	 passphrase             VARCHAR2(100),
	 source_directory 	VARCHAR2(50),
	 source_regexp   	VARCHAR2(100),
	 regexp_options		VARCHAR2(10)    DEFAULT 'i',
	 source_policy	        VARCHAR2(10) 	DEFAULT 'newest',
	 required       	VARCHAR2(1) 	DEFAULT 'Y',
	 reject_limit 		NUMBER,
	 dateformat		VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss am',
	 timestampformat	VARCHAR2(30)   	DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
	 delimiter		VARCHAR2(1)    	DEFAULT ',',
	 quotechar		VARCHAR2(2),
	 headers		VARCHAR2(1),
	 created_user   	VARCHAR2(30),
	 created_dt     	DATE,
	 modified_user  	VARCHAR2(30),
	 modified_dt    	DATE
       )
/

COMMENT ON TABLE filehub_conf IS 'table holding configuration information for PROCESSES in the FILE package';

COMMENT ON COLUMN filehub_conf.filehub_id IS 'sequence generated primary key of the table';
COMMENT ON COLUMN filehub_conf.filehub_name IS 'unique name for each distinct process';
COMMENT ON COLUMN filehub_conf.filehub_group IS 'logical grouping of filehub_ids that can be called together, such as files that are loaded in the same job';
COMMENT ON COLUMN filehub_conf.filehub_type IS 'type of filehub process; "external" for a feed going to an external table; "directory" for a feed going to a directory; and "extract" for an extract file being generated in a directory.';
COMMENT ON COLUMN filehub_conf.object_owner IS 'owner of the schema object associated with the file';
COMMENT ON COLUMN filehub_conf.object_name IS 'name of the schema object associated with the file';
COMMENT ON COLUMN filehub_conf.filename IS 'filename for the target of the process. For extracts, this is the name of the file to output to. For feeds, it is the filename to use in the directory or external table.';
COMMENT ON COLUMN filehub_conf.arch_directory IS 'name of the oracle directory object for an archive of the file';
COMMENT ON COLUMN filehub_conf.min_bytes IS 'minimum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN filehub_conf.max_bytes IS 'maximum size threshhold for the source file. A value of 0 ignores this requirement';
COMMENT ON COLUMN filehub_conf.file_datestamp IS 'NLS_DATE_FORMAT to use for the datestamp written on the file. A value of NA means that no timestamp will be written on the file.'; 
COMMENT ON COLUMN filehub_conf.source_regexp IS 'regular expression used to find files in SOURCE_DIR.';
COMMENT ON COLUMN filehub_conf.regexp_options IS 'additional match options that can specified in the regular expression';
COMMENT ON COLUMN filehub_conf.source_directory IS 'name of the directory object where the files are pulled from.';
COMMENT ON COLUMN filehub_conf.source_policy IS 'Action to take is multiple files match SOURCE_REGEXP. Current options are "newest","oldest","all","fail" or "proceed"';
COMMENT ON COLUMN filehub_conf.required IS 'Y/N column; determines whether the job fails or not when files are not found.';
COMMENT ON COLUMN filehub_conf.dateformat IS 'NLS_DATE_FORMAT of date columns in the extract';
COMMENT ON COLUMN filehub_conf.delimiter IS 'delimiter used to separate columns';
COMMENT ON COLUMN filehub_conf.quotechar IS 'quotechar used to support columns. A "none" specifies that no quotechar is used';
COMMENT ON COLUMN filehub_conf.headers IS 'a indicator of whether headers should be included as the first row in the file: "include" or "exclude"';
COMMENT ON COLUMN filehub_conf.baseurl IS 'the baseurl that the file is located at, which can be included in notifications';
COMMENT ON COLUMN filehub_conf.created_user IS 'for auditing';
COMMENT ON COLUMN filehub_conf.created_dt IS 'for auditing';
COMMENT ON COLUMN filehub_conf.modified_user IS 'for auditing';
COMMENT ON COLUMN filehub_conf.modified_dt IS 'for auditing';

ALTER TABLE filehub_conf ADD (
  CONSTRAINT filehub_conf_pk
 PRIMARY KEY
 (filehub_id)
    USING INDEX)
/

ALTER TABLE filehub_conf
      ADD CONSTRAINT filehub_conf_uk1 UNIQUE (filehub_name,filehub_group)
      USING INDEX
/

CREATE SEQUENCE filehub_conf_seq
/

GRANT SELECT ON filehub_conf TO filehub_sel
/