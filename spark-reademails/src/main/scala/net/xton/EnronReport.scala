package net.xton

import org.apache.spark.sql.{Dataset, SparkSession}

class EnronReport(val spark: SparkSession, basePath: String) {
  import spark.implicits._

  lazy val data: Dataset[MailRecord] =
    spark.sparkContext.wholeTextFiles(basePath)
      .filter(_._1.endsWith(".txt"))
      .map{ case (fileName,fileContents) =>
        MailRecord(fileContents,fileName) }
      .toDS()

}
