CREATE OR REPLACE TYPE efw.string_agg_type AS OBJECT (
   total   VARCHAR2 (4000),
   STATIC FUNCTION odciaggregateinitialize (
      sctx   IN OUT   string_agg_type)
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregateiterate (
      SELF    IN OUT   string_agg_type,
      VALUE   IN       VARCHAR2)
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregateterminate (
      SELF          IN       string_agg_type,
      returnvalue   OUT      VARCHAR2,
      flags         IN       NUMBER)
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregatemerge (
      SELF   IN OUT   string_agg_type,
      ctx2   IN       string_agg_type)
      RETURN NUMBER
);
/
