CREATE OR REPLACE TYPE tdinc.feed UNDER tdinc.fhconf (
   source_directory    VARCHAR2 (50),
   source_regexp       VARCHAR2 (100),
   regexp_options      VARCHAR2 (10),
   multi_file_action   VARCHAR2 (10),
   file_required       VARCHAR2 (8),
   MEMBER FUNCTION calc_rej_ind (p_rej_limit NUMBER DEFAULT 20)
      RETURN VARCHAR2,
   MEMBER PROCEDURE process_feed (p_keep_source BOOLEAN DEFAULT FALSE),
   MEMBER PROCEDURE audit_object
)
;
/