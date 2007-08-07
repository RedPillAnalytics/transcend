CREATE OR REPLACE TYPE BODY tdinc.notify
AS
   MEMBER PROCEDURE send
   AS
      l_recipients   VARCHAR2 (2000);
      l_sender       VARCHAR2 (50);
      o_app          applog        := applog (p_module       => 'notify.send',
                                              p_runmode      => SELF.runmode);
   BEGIN
      IF coreutils.is_true (notify_enabled)
      THEN
         CASE notify_method
            WHEN 'email'
            THEN
               SELECT recipients,
                      sender
                 INTO l_recipients,
                      l_sender
                 FROM email_notify_conf
                WHERE notify_id = SELF.notify_id;

               IF SELF.is_debugmode
               THEN
                  o_app.log_msg (   'Email Information:'
                                 || CHR (10)
                                 || 'Sender: '
                                 || l_sender
                                 || CHR (10)
                                 || 'Recipient: '
                                 || l_recipients
                                 || CHR (10)
                                 || 'Subject: '
                                 || subject
                                 || CHR (10)
                                 || 'Message: '
                                 || MESSAGE);
               ELSE
                  UTL_MAIL.send (sender          => l_sender,
                                 recipients      => l_recipients,
                                 subject         => subject,
                                 MESSAGE         => MESSAGE,
                                 mime_type       => 'text/html');
                  o_app.log_msg ('Email sent to: ' || l_recipients);
               END IF;
            ELSE
               raise_application_error (coreutils.get_err_cd ('notify_method_invalid'),
                                        coreutils.get_err_msg ('notify_method_invalid'));
         END CASE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END send;
END;
/