package net.xton

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpec}

trait UnitSpec extends WordSpec with Matchers  {
}

trait SparkUnitSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  private var _spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  lazy val spark: SparkSession = {
    if(_spark == null) {
      _spark = SparkSession.builder()
        .master("local[*]")
        .appName(getClass.getSimpleName)
        .getOrCreate()
    }
    _spark
  }

  override protected def afterAll(): Unit = {
    if(_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }

//  lazy val spark: SparkSession = {
//    TestHive.setConf("spark.sql.shuffle.partitions","4")
//    TestHive.sparkSession
//  }



}
