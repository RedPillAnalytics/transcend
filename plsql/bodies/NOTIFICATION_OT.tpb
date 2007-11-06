CREATE OR REPLACE TYPE BODY notification_ot
AS
   MEMBER PROCEDURE send( p_message VARCHAR2 DEFAULT NULL )
   AS
      e_smtp_error   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_smtp_error, -29279 );
      o_app          app_ot    := app_ot( p_module => 'send' );
   BEGIN
      IF td_ext.is_true( notification_enabled )
      THEN
         IF NOT td_inst.is_debugmode
         THEN
            CASE notification_method
               WHEN 'email'
               THEN
                  BEGIN
                     UTL_MAIL.send( sender          => sender,
                                    recipients      => recipients,
                                    subject         => subject,
                                    MESSAGE         => NVL( p_message, message1 ),
                                    mime_type       => 'text/html'
                                  );
                     td_inst.log_msg( 'Email sent to: ' || recipients );
                     td_inst.log_msg(    'Email Information:'
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
                        IF td_ext.is_true( notification_required )
                        THEN
                           raise_application_error
                                               ( td_inst.get_err_cd( 'utl_mail_error' ),
                                                    td_inst.get_err_msg( 'utl_mail_error' )
                                                 || ': '
                                                 || SQLERRM
                                               );
                        ELSE
                           td_inst.log_msg( 'The following SMTP error occured:' || SQLERRM
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