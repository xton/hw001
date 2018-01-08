package net.xton

class MailRecordTest extends UnitSpec {

  "a MailRecord" should {

    "extract" in {
      val fn = "115317.txt"
      val input = getClass.getResourceAsStream(s"/$fn")

      val mr = MailRecord(input,fn)

      mr.sender shouldBe "jeff.dasovich@enron.com"

      mr.recipients should contain theSameElementsAs Seq("jdasovic@enron.com")
      mr.day shouldBe "2000-09-14"

      mr.subject shouldBe "Observations on the Hearings this Week"
      mr.filename shouldBe fn

      mr.defects should be an 'empty

    }

  }
}
