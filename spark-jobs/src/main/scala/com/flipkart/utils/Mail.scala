package com.flipkart.utils

/**
 * Created by sharma.varun on 06/11/15.
 */
object Mail {

  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  implicit def liftToOption[T](t: T): Option[T] = Some(t)

  sealed abstract class MailType
  case object Plain extends MailType
  case object Rich extends MailType
  case object MultiPart extends MailType

  case class Mail(
                   from: (String, String), // (email -> name)
                   to: Seq[String],
                   cc: Seq[String] = Seq.empty,
                   bcc: Seq[String] = Seq.empty,
                   subject: String,
                   message: String,
                   richMessage: Option[String] = None,
                   attachment: Option[(java.io.File)] = None,
                   hostName: String,
                   username: String,
                   password: String
                   )

  object send {
    def a(mail: Mail) {
      import org.apache.commons.mail._

      val format =
        if (mail.attachment.isDefined) MultiPart
        else if (mail.richMessage.isDefined) Rich
        else Plain

      val commonsMail: Email = format match {
        case Plain => new SimpleEmail().setMsg(mail.message)
        case Rich => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
        case MultiPart => {
          val attachment = new EmailAttachment()
          attachment.setPath(mail.attachment.get.getAbsolutePath)
          attachment.setDisposition(EmailAttachment.ATTACHMENT)
          attachment.setName(mail.attachment.get.getName)
          new MultiPartEmail().attach(attachment).setMsg(mail.message)
        }
      }
      commonsMail.setHostName(mail.hostName)
      // TODO Set authentication from your configuration, sys properties or w/e
      commonsMail.setAuthentication(mail.username, mail.password)

      // Can't add these via fluent API because it produces exceptions
      mail.to foreach commonsMail.addTo
      mail.cc foreach commonsMail.addCc
      mail.bcc foreach commonsMail.addBcc

      commonsMail.
        setFrom(mail.from._1, mail.from._2).
        setSubject(mail.subject).
        send()
    }
  }
}