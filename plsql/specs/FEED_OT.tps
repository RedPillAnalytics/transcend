CREATE OR REPLACE TYPE feed_ot UNDER file_ot(
   source_directory   VARCHAR2( 50 ),
   source_dirpath     VARCHAR2( 200 ),
   source_regexp      VARCHAR2( 100 ),
   regexp_options     VARCHAR2( 10 ),
   source_policy      VARCHAR2( 10 ),
   required           VARCHAR2( 8 ),
   reject_limit       NUMBER,
   MEMBER PROCEDURE audit_ext_tab( p_num_lines NUMBER ),
   MEMBER PROCEDURE process( p_keep_source VARCHAR2 DEFAULT 'no' )
)
;
/