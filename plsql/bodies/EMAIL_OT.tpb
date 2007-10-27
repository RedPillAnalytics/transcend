CREATE OR REPLACE TYPE BODY email_ot
AS
   MEMBER PROCEDURE send
   AS
      e_smtp_error   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_smtp_error, -29279);
      o_app          app_ot    := app_ot (p_module => 'send');
   BEGIN
      IF td_ext.is_true (notify_enabled)
      THEN
         IF NOT td_inst.is_debugmode
         THEN
            BEGIN
               UTL_MAIL.send (sender          => sender,
                              recipients      => recipients,
                              subject         => subject,
                              MESSAGE         => MESSAGE,
                              mime_type       => 'text/html');
            EXCEPTION
               WHEN e_smtp_error
               THEN
                  td_inst.log_msg ('The following SMTP error occured:' || SQLERRM);
            END;
         END IF;

         td_inst.log_msg ('Email sent to: ' || recipients);
         td_inst.log_msg (   'Email Information:'
                        || CHR (10)
                        || 'Sender: '
                        || sender
                        || CHR (10)
                        || 'Recipient: '
                        || recipients
                        || CHR (10)
                        || 'Subject: '
                        || subject
                        || CHR (10)
                        || 'Message: '
                        || MESSAGE,
                        4);
      END IF;
   END send;
END;
/