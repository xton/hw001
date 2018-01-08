package net.xton

class EnronReportTest extends SparkUnitSpec {
  import spark.implicits._

  "EnronReport" should {
    val allFiles = Seq(
      "/10425.txt", "/106296.txt", "/106298.txt", "/106588.txt",
      "/106590.txt", "/109359.txt", "/110549.txt", "/113953.txt",
      "/114087.txt", "/114503.txt", "/115317.txt")

    "extract data" in {
      val dir = writeResourceToTempDirs("er","/115317.txt")

      val report = new EnronReport(spark,dir.getAbsolutePath)

      val sender = report.data.map(_.sender).head()

      sender shouldBe "jeff.dasovich@enron.com"

    }

    "extract moar data" in {
      val dir = writeResourceToTempDirs("larger", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      report.data.count() shouldBe 11L
    }

    "answer question 1" in {
      val dir = writeResourceToTempDirs("q1", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question1().take(4) shouldBe Seq(
        ("danny.mccarty@enron.com","2001-03-05",4L),
        ("darrell.schoolcraft@enron.com","2001-03-05",2L),
        ("darrell.schoolcraft@enron.com","2001-03-06",4L),
        ("doornbos@socrates.berkeley.edu","2000-08-04",2L) )

//      report.question1().foreach(println)

    }

    "answer question 2" in {
      val dir = writeResourceToTempDirs("q2", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question2()

      rs shouldBe (("jdasovic@enron.com",1L),("drew.fossum@enron.com",4L))
    }

    "answer question 3" in {
      val dir = writeResourceToTempDirs("q3.1", allFiles:_*)

      val report = new EnronReport(spark, dir.getAbsolutePath)

      val rs = report.question3_1()

      println("Did we get anything???")
      rs.foreach(println)

    }

  }
}
