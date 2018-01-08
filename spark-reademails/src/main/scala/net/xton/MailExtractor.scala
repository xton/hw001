package net.xton

import java.io.ByteArrayInputStream
import javax.mail.internet.MimeMessage

import org.apache.commons.mail.util.MimeMessageParser

class MailExtractor(raw: String) {
  def parse(): Unit = {
    val msg = new MimeMessage(null, new ByteArrayInputStream(raw.getBytes()))
    val parser = new MimeMessageParser(msg)
    parser.parse()
  }

}
