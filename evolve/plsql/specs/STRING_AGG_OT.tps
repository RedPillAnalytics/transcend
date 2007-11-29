CREATE OR REPLACE TYPE string_agg_ot AS OBJECT(
   total   VARCHAR2( 4000 ),
   STATIC FUNCTION odciaggregateinitialize( sctx IN OUT string_agg_ot )
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregateiterate( SELF IN OUT string_agg_ot, VALUE IN VARCHAR2 )
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregateterminate(
      SELF          IN       string_agg_ot,
      returnvalue   OUT      VARCHAR2,
      flags         IN       NUMBER
   )
      RETURN NUMBER,
   MEMBER FUNCTION odciaggregatemerge(
      SELF   IN OUT   string_agg_ot,
      ctx2   IN       string_agg_ot
   )
      RETURN NUMBER
);
/