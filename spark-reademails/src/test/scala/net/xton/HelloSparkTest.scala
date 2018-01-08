package net.xton

import org.apache.spark.sql.functions._
import org.apache.spark.sql._


class HelloSparkTest extends SparkUnitSpec {
  import spark.implicits._

  "a test of my setup" should {
    "say hello" in {
      new HelloSpark().hello() shouldBe "Hello World!"
    }

    "say hello in spark" in {

      val ds = Seq(
        ("hi",2),
        ("hit",3),
        ("height",6) ).toDS()

      val rs = ds.agg(count($"*").as[Long]).as[Long].collect()

      rs shouldBe List(3)
    }

    "say hello with real files" in {
      val dir = writeResourceToTempDirs("hello","/words.txt")

      val df = spark.read.textFile(dir.getAbsolutePath)
        .flatMap(_.split(" +")).toDF("word")


      val rs = df.groupBy('word)
        .agg(count("*") as "tally")
        .orderBy($"tally".desc)
        .take(2)

      rs.map(_.getString(0)) should contain theSameElementsAs List("I", "the")

      rs.foreach(println)
    }

    "reads a whole file" in {
      val dir = writeResourceToTempDirs("hello","/words.txt")

      val rs = spark.sparkContext.wholeTextFiles(dir.getAbsolutePath)
        .flatMap(_._2.split(" +"))
        .toDF("word")
        .groupBy('word)
        .agg(count("*") as 'tally)
        .orderBy($"tally".desc)
        .take(2)

      rs.foreach(println)
      rs.map(_.getString(0)) should contain theSameElementsAs List("I", "the")

    }
  }

}
