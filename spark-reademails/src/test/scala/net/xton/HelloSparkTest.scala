package net.xton

import org.apache.spark.sql.functions._
import org.apache.spark.sql._


class HelloSparkTest extends SparkUnitSpec {

  "a thing" should {
    "say hello" in {
      new HelloSpark().hello() shouldBe "Hello World!"
    }

    "say hello in spark" in {
      import spark.implicits._

      val ds = Seq(
        ("hi",2),
        ("hit",3),
        ("height",6) ).toDS()

      val rs = ds.agg(count($"*").as[Long]).as[Long].collect()

      rs shouldBe List(3)
    }
  }

}
