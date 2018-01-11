package net.xton

import java.sql.{Date, Timestamp}
import java.time.Duration

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import scala.collection.mutable

class EnronReport(val spark: SparkSession, basePath: String) {
  import spark.implicits._

  lazy val data: Dataset[MailRecord] =
    spark.sparkContext.wholeTextFiles(basePath)
      .filter(_._1.endsWith(".txt"))
      .map{ case (fileName,fileContents) =>
        MailRecord(fileContents,fileName) }
      .toDS()

  lazy val tighterData: Dataset[MailRecord] = data.coalesce(20).as('cachedData).cache()

  /** Count of emails received per user per day */
  def question1(ds: Dataset[MailRecord] = tighterData): Seq[(String,String,Long)] =
    ds.flatMap(r => r.recipients.map((_,r.day)))
      // yield (name,day) for each recipient
      .toDF("name","day")
      .groupBy($"name",$"day")
      .agg(count("*"))
      .sort($"name",$"day")
      .as[(String,String,Long)]
      .collect()
      .toSeq

  def question2(ds: Dataset[MailRecord] = tighterData): ((String, Long), (String, Long)) = {
    val mostDirected = ds
      .filter(_.recipients.length == 1)
      .map(_.recipients.head)
      .groupByKey(identity).count()
      .toDF("name","tally").sort($"tally".desc)
      .as[(String,Long)].head()

    val biggestSpammer = ds
      .filter(_.recipients.length > 1)
      .map(_.sender)
      .groupByKey(identity).count()
      .toDF("name","tally").sort($"tally".desc)
      .as[(String,Long)].head()

    (mostDirected,biggestSpammer)
  }

  /** try simple approach of stripping Re:'s to match */
  def question3_1(ds: Dataset[MailRecord] = tighterData): Array[(MailRecord,MailRecord,Long)] = {
    ds.groupByKey(_.subject.replaceFirst(raw"^(?i:re:\s*)+",""))
      .flatMapGroups( (name,records) => EnronReport.findReplies(records))
      .toDF("original","reply","delta").sort($"delta")
      .as[(MailRecord,MailRecord,Long)]
      .take(5)
  }

  /** try simple approach of stripping Re:'s to match */
  def question3_2(ds: Dataset[MailRecord] = tighterData): Array[(MailRecord,MailRecord,Long)] = {
    ds.toDF().createOrReplaceTempView("dat")
    spark.sql(
      """
        |
      """.stripMargin)


    ds.groupByKey(_.subject.replaceFirst(raw"^(?i:re:\s*)+",""))
      .flatMapGroups( (name,records) => EnronReport.findReplies(records))
      .toDF("original","reply","delta").sort($"delta")
      .as[(MailRecord,MailRecord,Long)]
      .take(5)

  }

}

object EnronReport {

  /**
    *
    * @param records
    * @param maxLag
    * @return (original, reply, replyLag)
    */
  def findReplies(records:TraversableOnce[MailRecord],maxLag:Long = 20L*60*1000): Vector[(MailRecord, MailRecord, Long)] = {

    val reverseSorted = records.toVector.sortBy(_.date)
    val danglingOriginals = mutable.HashMap.empty[(String,String),MailRecord]

//    println(reverseSorted.map(_.filename))

    reverseSorted.flatMap { record =>
      val rs = record.recipients.flatMap { recipient =>
        danglingOriginals.remove(recipient,record.sender) match {
            // TODO: making assumption about allrecipients == original thread starter here... probably not valid
          case Some(original) if record.date - original.date < maxLag =>
//            println(s"${original.filename} -> ${record.filename}")
            Seq((original, record, record.date - original.date))
          case _ => Nil
        }
      }
      record.recipients.foreach(r => danglingOriginals((record.sender,r)) = record)
      rs
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
       //   .enableHiveSupport()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val report = new EnronReport(spark,args(0))
    val q1 = report.question1()
    val q2 = report.question2()
    val q3 = report.question3_1()
    val defects = report.data.flatMap( r => r.defects.map((_,r.filename))).take(100)


    println()
    println("Daily Counts:")
    for((person,day,count) <- q1) {
      println("% 8d : %s - %s".format(count,person,day))
    }

    val (mostDirected,biggestSpammer) = q2
    println()
    println("Most Broadcasts Sent: %s (%d)".format(biggestSpammer.productIterator.toSeq:_*))
    println("Most Directs Received: %s (%d)".format(mostDirected.productIterator.toSeq:_*))

    println()
    println("Fastest Replies:")
    for(((original,reply,lag),idx) <- q3.zipWithIndex) {
      println("%s: %s [%s] - %s [%s] <= %s [%s - %s]".format(
        idx+1,
        java.time.Instant.ofEpochMilli(original.date).toString,
        Duration.ofMillis(lag).toString,
        original.subject, original.filename,
        reply.subject, java.time.Instant.ofEpochMilli(reply.date).toString, reply.filename))
    }

    println()
    println("DEFECTS:")
    defects.foreach(println)

  }
}
