CREATE OR REPLACE PACKAGE mail_tools
AS
-- SENDMAIL supports variable message length with/without attachments
-- QUERY_SERVER allows you to check the status of a mail server to see if it is running
-- DUMP_FLATFILE allows you to dump flat file data from query submitted
--
--
-- Query Server to verify that the server is up and running.
-- Connects, Noop Command is executed, Disconnect.

   --
-- GET_MAIL_ADDRESS is utilized in the SENDMAIL procedure
--
--
--
-- Table 100-3 SMTP Reply Codes
-- Reply Code
-- Meaning
--
-- 211 System status, or system help reply
--
-- 214 Help message [Information on how to use the receiver or the meaning of a particular non-standard command; this reply is useful only to the human user]
--
-- 220 <domain> Service ready
--
-- 221 <domain> Service closing transmission channel
--
-- 250 Requested mail action okay, completed
--
-- 251 User not local; will forward to <forward-path>
--
-- 252 OK, pending messages for node <node> started. Cannot VRFY user (e.g., info is not local), but will take message for this user and attempt delivery.
--
-- 253 OK, <messages> pending messages for node <node> started
--
-- 354 Start mail input; end with <CRLF>.<CRLF>
--
-- 355 Octet-offset is the transaction offset
--
-- 421 <domain> Service not available, closing transmission channel (This may be a reply to any command if the service knows it must shut down.)
--
-- 450 Requested mail action not taken: mailbox unavailable [for example, mailbox busy]
--
-- 451 Requested action aborted: local error in processing
--
-- 452 Requested action not taken: insufficient system storage
--
-- 453 You have no mail.
--
-- 454 TLS not available due to temporary reason. Encryption required for requested authentication mechanism.
--
-- 458 Unable to queue messages for node <node>
--
-- 459 Node <node> not allowed: reason
--
-- 500 Syntax error, command unrecognized (This may include errors such as command line too long.)
--
-- 501 Syntax error in parameters or arguments
--
-- 502 Command not implemented
--
-- 503 Bad sequence of commands
--
-- 504 Command parameter not implemented
--
-- 521 <Machine> does not accept mail.
--
-- 530 Must issue a STARTTLS command first. Encryption required for requested authentication mechanism.
--
-- 534 Authentication mechanism is too weak.
--
-- 538 Encryption required for requested authentication mechanism.
--
-- 550 Requested action not taken: mailbox unavailable [for , mailbox not found, no access]
--
-- 551 User not local; please try <forward-path>
--
-- 552 Requested mail action aborted: exceeded storage allocation
--
-- 553 Requested action not taken: mailbox name not allowed [for example, mailbox syntax incorrect]
--
-- 554 Transaction failed
--
--
--
/*
 This version allows for a customized seperator value. Using this function will allow you to
 perform fixed width flat files by defining '' for no seperator and then RPAD/LPAD your columns as necessary.
 Or use whatever seperator you wish to use, pipe, space, zeros, etc.
 */

   --  Example : This will generate a flat file which tabbed seperated
--
-- DECLARE
--    l_rows   NUMBER;
--    l_sql    VARCHAR2(32000);
-- BEGIN
--    l_sql := '
-- SELECT   rpad(hou.NAME,70) udn_desc
-- ,        rpad(pcak.segment1,6) coid
-- ,        rpad(pcak.segment2,4) udn
--     FROM hr_all_organization_units hou, hr.pay_cost_allocation_keyflex pcak
--    WHERE TRUNC (SYSDATE) BETWEEN hou.date_from
--                              AND NVL (hou.date_to, ''31-DEC-4712'')
--      AND pcak.cost_allocation_keyflex_id = hou.cost_allocation_keyflex_id
-- GROUP BY pcak.segment1, pcak.segment2, hou.NAME
-- ORDER BY 1, 2, 3
-- ';
--
--    l_rows :=
--       dump_flatfile
--          (p_query          =>
-- ,         p_dir            => '/xfer'
-- ,         p_filename       => 'test.csv'
-- ,       p_separator     => '     ' -- <= tabbed 5 spaces between each column
-- ,       p_max_linesize   => 32000
-- ,       p_mode       => 'w' -- (w)rite mode or (a)ppend mode
--          );
-- END;
   FUNCTION dump_flatfile (
      p_query                    IN       VARCHAR2
,     p_dir                      IN       VARCHAR2
,     p_filename                 IN       VARCHAR2
,     p_separator                IN       VARCHAR2
,     p_headers                  IN       BOOLEAN DEFAULT FALSE
,     p_trailing_separator       IN       BOOLEAN DEFAULT FALSE
,     p_max_linesize             IN       NUMBER DEFAULT 32000
,     p_mode                     IN       VARCHAR2 DEFAULT 'w' )
      RETURN NUMBER;

   FUNCTION get_mail_address (
      addr_list                  IN OUT   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION smtp_command (
      command                    IN       VARCHAR2
,     ok                         IN       VARCHAR2 DEFAULT '250'
,     code                       OUT      VARCHAR2
,     DEBUG                               NUMBER DEFAULT 0 )
      RETURN BOOLEAN;

   FUNCTION query_server (
      smtp_server                         VARCHAR2
,     smtp_server_port                    PLS_INTEGER DEFAULT 25
,     DEBUG                               NUMBER DEFAULT 0 )
      RETURN BOOLEAN;

/*
This procedure uses the UTL_TCP package to send an email message.
Up to three file names may be specified as attachments.

Written: Dave Wotton, 14/6/01 (Cambridge UK)
This script comes with no warranty or support. You are free to
modify it as you wish, but please retain an acknowledgement of
my original authorship.

Amended: Dave Wotton, 10/7/01
Now uses the utl_smtp.write_data() method to send the message,
eliminating the 32Kb message size constraint imposed by the
utl_smtp.data() procedure.

Amended: Dave Wotton, 20/7/01
Increased the v_line variable, which holds the file attachment
lines from 400 to 1000 bytes. This is the maximum supported
by RFC2821, The Simple Mail Transfer Protocol specification.

Amended: Dave Wotton, 24/7/01
Now inserts a blank line before each MIME boundary line. Some
mail-clients require this.

Amended: Dave Wotton, 4/10/01
Introduced a 'debug' parameter. Defaults to 0. If set to
non-zero then errors in opening files for attaching are
reported using dbms_output.put_line.
Include code to hand MS Windows style pathnames.

Amended: Barry Chase, 4/29/03
Added Priority to procedure and also X-Mailer ID.
Removed restrictions for email size limitation as well.
Emails are now formatted text messages, meaning you can
write your message in html format.
And finally, changed from using UTL_SMTP to UTL_TCP instead.

Amended: Barry Chase 11/10/2003
Added session timeout of 4 minutes to prevent hanging server connections

Amended: Barry Chase 12/04/2003
Added Date String so that it represents timezone of originating server
p_datestring

Amended: Barry Chase 03/01/2004
Added functionality to support binary attachments and remote attachments.
Its about 98% complete. Not work perfectly yet. Still trying to figure out
encoding to base64 or mime. Have a good start on it though.

04/12/2004
BCHASE :: Binary Support is fully functional now.

09/01/2005
BCHASE :: Modified attachment directories to use DBA_DIRECTORIES instead
of UTL_DIR in the Oracle initialization file.

02/22/2006
BCHASE :: Added variable length message email support (CLOB)

04/21/2006
BCHASE :: Expanded functionality to include Cc and Bcc
Also removed redundant calls from package. The single
mail_files command will handle flat files and binary files such as zip/pdf/etc.

SMTP Server and SMTP Server Port are parameters on the sendmail procedure now
as well.

Refer to http://home.clara.net/dwotton/dba/oracle_smtp.htm for more
details on the original source code.

For information on the enhanced mail_tools package as provided by Barry
Chase, refer to http://www.myoracleportal.com

*/

   /* Retrieves local binary file from database server.
    * using DBMS_LOB commands and stores into BLOB
    *
    * return BLOB
   */
   FUNCTION get_local_binary_data (
      p_dir                      IN       VARCHAR2
,     p_file                     IN       VARCHAR2 )
      RETURN BLOB;

/* Supports binary attachments and message of variable length. Uses CLOB.*/

-- DECLARE
-- t_blob BLOB;
-- BEGIN
--
-- Use the get_local_binary_data to collect your BLOB from the filesystem
-- or just load from a table where your BLOB is stored at, then just pass
-- as t_blob on the binaryfile parameter below. Remember to provide an
-- appropriate filename. Optionally, you can leave filename NULL and pass
-- the binaryfile parameter as EMPTY_BLOB() to send an email without an
-- attachment.
--
--   t_blob :=
--    mail_tools.get_local_binary_data
--                   ( p_dir =>                         'INTF0047_TABLES'
--,                    p_file =>                        'test_file1.csv' );
--    mail_tools.sendmail
--             ( smtp_server =>                   'your.smtp.server'
-- ,             smtp_server_port =>              25
-- ,             from_name =>                     'Email Address of Sender'
-- ,             to_name =>                       'list of TO email addresses separated by commas (,)'
-- ,             cc_name =>                       'list of CC email addresses separated by commas (,)'
-- ,             bcc_name =>                      'list of BCC email addresses separated by commas (,)'
-- ,             subject =>                       'Some brief Subject'
-- ,             MESSAGE =>                       'Your message goes here. Can include HTML code.'
-- ,             priority =>                      '1-5 1 being the highest priority and 3 normal priority'
-- ,             filename =>                      'your.filename.txt or leave NULL'
-- ,             binaryfile =>                    'your blob is passed here otherwise leave as EMPTY_BLOB()
-- ,             DEBUG =>                         'Default is DBMS output otherwise pass a 1 to disable );
-- END;
--
   PROCEDURE sendmail (
      smtp_server                         VARCHAR2
,     smtp_server_port                    PLS_INTEGER DEFAULT 25
,     from_name                           VARCHAR2
,     to_name                             VARCHAR2
,     cc_name                             VARCHAR2 DEFAULT NULL
,     bcc_name                            VARCHAR2 DEFAULT NULL
,     subject                             VARCHAR2
,     MESSAGE                             CLOB
,     priority                            PLS_INTEGER DEFAULT NULL
,     filename                            VARCHAR2 DEFAULT NULL
,     binaryfile                          BLOB DEFAULT EMPTY_BLOB ( )
,     DEBUG                               NUMBER DEFAULT 0 );

   v_parm_value                  VARCHAR2 ( 4000 );
   lbok                          BOOLEAN;
   v_smtp_server                 VARCHAR2 ( 50 );
   v_smtp_server_port            NUMBER := 25;
   crlf                          VARCHAR2 ( 10 ) := utl_tcp.crlf;
   conn                          utl_tcp.connection;
   p_debug_marker                PLS_INTEGER := 0;
   rc                            INTEGER;
   p_from_name                   VARCHAR2 ( 100 );
   p_to_name                     VARCHAR2 ( 4000 );
   p_cc_name                     VARCHAR2 ( 4000 );
   p_bcc_name                    VARCHAR2 ( 4000 );
   p_subject                     VARCHAR2 ( 150 );
   tx_timeout                    PLS_INTEGER := 240;
                                                  -- 240 Seconds (4 minutes);
--
   p_datestring                  VARCHAR2 ( 100 )
      :=    'Date: '
         || TO_CHAR ( SYSDATE, 'MM/DD/RR HH:MI AM' )
         || ' '
         || DBTIMEZONE
         || ' '
         || '(GMT'
         || DBTIMEZONE
         || ')';
   -- Customize the signature that will appear in the email's MIME header.
   -- Useful for versioning.
   mailer_id            CONSTANT VARCHAR2 ( 256 ) := 'Mailer by Oracle UTL_TCP';
   max_base64_line_width CONSTANT PLS_INTEGER := 76 / 4 * 3;
END;
/