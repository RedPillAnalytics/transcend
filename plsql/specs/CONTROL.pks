CREATE OR REPLACE PACKAGE tdinc.control
IS
   PROCEDURE set_logging_level (
      p_module          VARCHAR2 DEFAULT 'default',
      p_action          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4);

   PROCEDURE set_runmode (
      p_module            VARCHAR2 DEFAULT 'default',
      p_action            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime');

   PROCEDURE set_registration (
      p_module         VARCHAR2 DEFAULT 'default',
      p_action         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'register');
END control;
/