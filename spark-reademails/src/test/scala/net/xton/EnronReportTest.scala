package net.xton

class EnronReportTest extends SparkUnitSpec {
  import spark.implicits._

  "EnronReport" should {

    "extract data" in {
      val dir = writeResourceToTempDirs("er","/115317.txt")

      val report = new EnronReport(spark,dir.getAbsolutePath)

      val sender = report.data.map(_.sender).head()

      sender shouldBe "jeff.dasovich@enron.com"

    }

  }
}
