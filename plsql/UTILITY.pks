CREATE OR REPLACE PACKAGE common.utility
AS
   FUNCTION get_package_name
      RETURN VARCHAR2;

   FUNCTION whence
      RETURN VARCHAR2;

   FUNCTION get_dir_path(
      p_dirname   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_dir_name(
      p_dir_path   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_numlines(
      p_dirname    IN   VARCHAR2,
      p_filename   IN   VARCHAR2 )
      RETURN NUMBER;

   PROCEDURE send_email(
      p_recipients      IN   VARCHAR2,
      p_sender          IN   VARCHAR2 DEFAULT 'no-reply@transcendentdata.com',
      p_subject         IN   VARCHAR2,
      p_message         IN   VARCHAR2,
      p_smtp_hostname   IN   VARCHAR2 DEFAULT 'localhost',
      p_smtp_portnum    IN   VARCHAR2 DEFAULT '25',
      p_pre_html             BOOLEAN DEFAULT FALSE );

   FUNCTION format_url(
      p_url   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION unzip_file(
      p_dirpath    VARCHAR2,
      p_filename   VARCHAR2,
      p_debug      BOOLEAN DEFAULT FALSE )
      RETURN VARCHAR2;

   FUNCTION decrypt_file(
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_debug        BOOLEAN DEFAULT FALSE )
      RETURN VARCHAR2;
END utility;
/