CREATE OR REPLACE FUNCTION stragg( input VARCHAR2 )
   RETURN VARCHAR2 PARALLEL_ENABLE AGGREGATE
   USING string_agg_ot;
/