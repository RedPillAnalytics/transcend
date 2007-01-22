CREATE OR REPLACE TYPE BODY efw.string_agg_type
IS
   STATIC FUNCTION odciaggregateinitialize (
      sctx   IN OUT   string_agg_type)
      RETURN NUMBER
   IS
   BEGIN
      sctx := string_agg_type (NULL);
      RETURN odciconst.success;
   END;
   MEMBER FUNCTION odciaggregateiterate (
      SELF    IN OUT   string_agg_type,
      VALUE   IN       VARCHAR2)
      RETURN NUMBER
   IS
   BEGIN
      SELF.total := SELF.total || ',' || VALUE;
      RETURN odciconst.success;
   END;
   MEMBER FUNCTION odciaggregateterminate (
      SELF          IN       string_agg_type,
      returnvalue   OUT      VARCHAR2,
      flags         IN       NUMBER)
      RETURN NUMBER
   IS
   BEGIN
      returnvalue := LTRIM (SELF.total, ',');
      RETURN odciconst.success;
   END;
   MEMBER FUNCTION odciaggregatemerge (
      SELF   IN OUT   string_agg_type,
      ctx2   IN       string_agg_type)
      RETURN NUMBER
   IS
   BEGIN
      SELF.total := SELF.total || ctx2.total;
      RETURN odciconst.success;
   END;
END;
/
