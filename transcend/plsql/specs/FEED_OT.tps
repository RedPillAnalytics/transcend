CREATE OR REPLACE TYPE feed_ot UNDER file_label_ot(
   source_regexp      VARCHAR2( 100 ),
   match_parameter    VARCHAR2( 10 ),
   source_policy      VARCHAR2( 10 ),
   required           VARCHAR2( 8 ),
   delete_source      VARCHAR2( 3 ),
   CONSTRUCTOR FUNCTION feed_ot(
      p_file_label	   VARCHAR2,
      p_source_directory   VARCHAR2
   )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE verify,
   OVERRIDING MEMBER PROCEDURE process
)
;
/