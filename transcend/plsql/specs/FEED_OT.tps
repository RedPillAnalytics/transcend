CREATE OR REPLACE TYPE feed_ot UNDER file_ot(
   source_directory   VARCHAR2( 50 ),
   source_dirpath     VARCHAR2( 200 ),
   source_regexp      VARCHAR2( 100 ),
   match_parameter    VARCHAR2( 10 ),
   source_policy      VARCHAR2( 10 ),
   required           VARCHAR2( 8 ),
   delete_source      VARCHAR2( 3 ),
   reject_limit       NUMBER,
   CONSTRUCTOR FUNCTION feed_ot(
      p_file_group   VARCHAR2,
      p_file_label   VARCHAR2
   )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE verify,
   MEMBER PROCEDURE audit_ext_tab( p_num_lines NUMBER ),
   MEMBER PROCEDURE process
)
;
/