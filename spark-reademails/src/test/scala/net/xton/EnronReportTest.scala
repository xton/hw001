package net.xton

class EnronReportTest extends SparkUnitSpec {
  import spark.implicits._

  "EnronReport" should {
    val allFiles = Seq(
      "/10425.txt", "/106296.txt", "/106298.txt", "/106588.txt",
      "/106590.txt", "/109359.txt", "/110549.txt", "/113953.txt",
      "/114087.txt", "/114503.txt", "/115317.txt")

    val threadedFiles = Seq("/228996.txt","/228911.txt","/122923.txt","/122926.txt")

    "extract one record" in {
      val dir = writeResourceToTempDirs("er","/115317.txt")

      val report = new EnronReport(spark,dir.getAbsolutePath)

      val sender = report.data.map(_.sender).head()

      sender shouldBe "jeff.dasovich@enron.com"

      // TODO: test all attributes
    }

    "extract moar data" in {
      /**
        * Make sure we can run the extract function on a larger set of real data.
        * For production code, we'd more carefully test all edge cases and
        * default values and validate each one.
        *
        * For this exercise, we're happy to just make sure the code doesn't explode
        * and is consistent with our alternate implementation.
        */
      val dir = writeResourceToTempDirs("larger", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      report.data.count() shouldBe 11L

      // TODO: test all attributes
    }

    "answer question 1" in {
      val dir = writeResourceToTempDirs("q1", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question1().take(4) should contain theSameElementsAs Seq(
        ("danny.mccarty@enron.com","2001-03-05",2L),
        ("darrell.schoolcraft@enron.com","2001-03-05",2L),
        ("darrell.schoolcraft@enron.com","2001-03-06",2L),
        ("doornbos@socrates.berkeley.edu","2000-08-04",1L) )

//      report.question1().foreach(println)

    }

    "answer question 2" in {
      val dir = writeResourceToTempDirs("q2", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question2()

      rs shouldBe (("jdasovic@enron.com",1L),("drew.fossum@enron.com",4L))
    }

    "answer question 3" in {
      val dir = writeResourceToTempDirs("q3.1", allFiles ++ threadedFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question3_1()

      // NB: this is a verrrry basic test. For production code we'd have much more elaborate
      //     testing here. For this quick project I've just manually confirmed that both the python
      //     and spark versions produce the same results.

      rs.map(_._3) shouldBe Array(236000L,240000L)

    }

    "answer question 3 with just sql" in {
      val dir = writeResourceToTempDirs("q3.2", allFiles ++ threadedFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question3_2()

      rs.map(_._3) shouldBe Array(236000L,240000L)

    }

  }

  "the findReplies function" should {
    import EnronReport._

    val proto = MailRecord("bob",Vector("alice","darren"),5L,"2001-01-05","hello, this is bob","bobs.txt",Nil)
    val original = proto
    val reply = proto.copy(sender = "alice", recipients = Vector("bob"), date = 9L, filename = "alices.txt")
    val reply2 = proto.copy(sender = "alice", recipients = Vector("bob"), date = 10L, filename = "alices2.txt")
    val nonreply = proto.copy(sender = "corey", recipients = Vector("bob"), date = 7L, filename = "coreys.txt")
    val otherreply = proto.copy(sender = "darren", recipients = Vector("bob"), date = 13L, filename = "darrens.txt")

    val t2 = MailRecord("george",Vector("alice","darren"),100L,"2001-01-05","hello, this is george","georges.txt",Nil)
    val t2reply = t2.copy(sender = "alice", recipients = Vector("george"), date = 109L, filename = "alicestogeorge.txt")

    "handle nothing" in {
      findReplies(Nil) should be an 'empty
    }

    "handle a single record" in {
      findReplies(proto :: Nil) should be an 'empty
    }

    "link two related records" in {
      findReplies(original :: reply :: Nil) shouldBe Vector((original,reply,4L))

      findReplies(reply :: original :: Nil) shouldBe Vector((original,reply,4L))
    }

    "chose the right original with competition" in {
      findReplies(original :: nonreply :: reply2 :: reply :: Nil) shouldBe Vector((original,reply,4L))
    }

    "produce multiple replies for one original" in {
      findReplies(original :: nonreply :: reply2 :: reply :: otherreply :: Nil) shouldBe
        Vector((original,reply,4L), (original,otherreply,8L))

    }

    "handle multiple threads" in {
      findReplies(original :: t2 :: reply2 :: t2reply :: Nil) shouldBe
        Vector((original,reply2,5L), (t2,t2reply,9L))
    }

    "ignore records two far apart" in {
      findReplies(original :: reply :: Nil, maxLag = 2L) should be an 'empty
    }

  }

}
