CREATE OR REPLACE TYPE BODY notification_ot
AS
   CONSTRUCTOR FUNCTION notification_ot
      RETURN SELF AS RESULT
   AS
   BEGIN
      module := td_inst.module;
      action := td_inst.action;
   END;
   MEMBER PROCEDURE send( p_message VARCHAR2 DEFAULT NULL )
   AS
      e_smtp_error   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_smtp_error, -29279 );
   BEGIN
      IF td_core.is_true( enabled )
      THEN
         IF NOT evolve_log.is_debugmode
         THEN
            CASE method
               WHEN 'email'
               THEN
                  BEGIN
                     UTL_MAIL.send( sender          => sender,
                                    recipients      => recipients,
                                    subject         => subject,
                                    MESSAGE         => NVL( p_message, MESSAGE ),
                                    mime_type       => 'text/html'
                                  );
                     evolve_log.log_msg( 'Email sent to: ' || recipients );
                     evolve_log.log_msg(    'Email Information:'
                                         || CHR( 10 )
                                         || 'Sender: '
                                         || sender
                                         || CHR( 10 )
                                         || 'Recipient: '
                                         || recipients
                                         || CHR( 10 )
                                         || 'Subject: '
                                         || subject
                                         || CHR( 10 )
                                         || 'Message: '
                                         || p_message,
                                         4
                                       );
                  EXCEPTION
                     WHEN e_smtp_error
                     THEN
                        IF td_core.is_true( required )
                        THEN
                           raise_application_error
                                               ( td_inst.get_err_cd( 'utl_mail_error' ),
                                                    td_inst.get_err_msg( 'utl_mail_error' )
                                                 || ': '
                                                 || SQLERRM
                                               );
                        ELSE
                           evolve_log.log_msg(    'The following SMTP error occured:'
                                               || SQLERRM
                                             );
                        END IF;
                  END;
               ELSE
                  raise_application_error( td_inst.get_err_cd( 'notify_method_invalid' ),
                                           td_inst.get_err_msg( 'notify_method_invalid' )
                                         );
            END CASE;
         END IF;
      END IF;
   END send;
END;
/

SHOW errors