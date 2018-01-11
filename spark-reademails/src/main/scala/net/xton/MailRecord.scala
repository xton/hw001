package net.xton

import java.io.{ByteArrayInputStream, InputStream}
import javax.mail.internet.MimeMessage

import org.apache.commons.mail.util.MimeMessageParser
import java.text.SimpleDateFormat
import java.util.Locale

/** This case class determines our data model for all future queries */
case class MailRecord(
  sender: String,
  recipients: Vector[String],
  date: Long,
  day: String,
  subject: String,
  filename: String,
  defects: Seq[String]
) {

  lazy val words = Set(subject.split(raw"\s+"):_*)
}

object MailRecord {
  /** factory method for creating MailRecords from raw text
    *
    * implements default values for missing elements and builds
    * up an array of defects when that happens.
    **/
  def apply(input: InputStream,filename:String): MailRecord = {
    var defects = Nil:List[String]

    /* tiny helper for errorchecking */
    def orDefault[U](value: U, default: =>U, name: String): U = Option(value) match {
      case Some(v) => v
      case None =>
        defects ::= s"Missing $name"
        default
    }

    val msg = new MimeMessage(null, input)
    val parser = new MimeMessageParser(msg)
    parser.parse()

    val sentDate = orDefault(msg.getSentDate,new java.util.Date(),"date")
    val formatter = new SimpleDateFormat("yyyy-MM-dd",Locale.US)

    val from = Option(msg.getFrom) match {
      case Some(Array(firstFrom, _*)) =>
        firstFrom.toString
      case _ =>
        defects ::= "No or empty FROM field!"
        "(no from)"
    }

    val recipients = Option(msg.getAllRecipients) match {
      case Some(rs) if rs.nonEmpty =>
        rs.map(_.toString).distinct
      case _ =>
        defects ::= "No or empty recipients!"
        Array.empty[String]
    }

    new MailRecord(
      from,
      recipients.toVector,
      sentDate.getTime,
      formatter.format(sentDate),
      orDefault(msg.getSubject,"(no subject)","subject"),
      filename,
      defects)
  }

  def apply(string: String, filename: String): MailRecord =
    apply(new ByteArrayInputStream(string.getBytes()),filename)
}
