package net.xton

import java.sql.{Date, Timestamp}

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

  /** Count of emails received per user per day */
  def question1(ds: Dataset[MailRecord] = data): Seq[(String,String,Long)] =
    ds.flatMap(r => r.recipients.map((_,r.day)))
      // yield (name,day) for each recipient
      .toDF("name","day")
      .groupBy($"name",$"day")
      .agg(count("*"))
      .sort($"name",$"day")
      .as[(String,String,Long)]
      .collect()
      .toSeq

  def question2(ds: Dataset[MailRecord] = data): ((String, Long), (String, Long)) = {
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

  // 20 milliseconds
  val maxLagTime: Long = 20L*60*1000

  /** try simple approach of stripping Re:'s to match */
  def question3_1(ds: Dataset[MailRecord] = data): Array[(MailRecord,MailRecord,Long)] = {
    val results = data.groupByKey(_.subject.replaceFirst(raw"^(?i:re:\s*)+",""))
      .flatMapGroups{ (group,records) =>
        val reverseSorted = records.toVector.sortBy(_.date)(Ordering[Long].reverse)
        val danglingOriginals = mutable.HashMap.empty[String,MailRecord]
        val replies = List.empty[(MailRecord,MailRecord)]

        reverseSorted.flatMap{ record =>
          val rs = record.recipients.flatMap{ recipient =>
            danglingOriginals.remove(recipient) match {
              case Some(original) if record.date - original.date < maxLagTime =>
                Seq((original,record,record.date - original.date))
              case _ => Nil
            }
          }
          danglingOriginals(record.sender) = record
          rs
        }
      }
      .toDF("original","reply","delta").sort($"delta".desc)
      .as[(MailRecord,MailRecord,Long)]
      .take(5)

    results
  }

}
