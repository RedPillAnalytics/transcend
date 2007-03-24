CREATE OR REPLACE TYPE tdinc.feed UNDER tdinc.fhconf (
   source_directory   VARCHAR2 (50),
   source_dirpath     VARCHAR2 (200),
   source_regexp      VARCHAR2 (100),
   regexp_options     VARCHAR2 (10),
   source_policy      VARCHAR2 (10),
   required           VARCHAR2 (8),
   MEMBER PROCEDURE process_feed (p_keep_source BOOLEAN DEFAULT FALSE)
)
;
/