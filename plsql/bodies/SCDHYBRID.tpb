CREATE OR REPLACE TYPE BODY tdinc.scdhybrid
AS
   -- store audit information about the feed or extract
   MEMBER PROCEDURE process
   AS
      o_app   applog := applog (p_module => 'scdhybrid.process', p_runmode => SELF.runmode);
   BEGIN
      NULL;
   END process;
END;
/