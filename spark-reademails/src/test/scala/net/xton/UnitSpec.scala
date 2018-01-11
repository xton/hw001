package net.xton

import java.io.{File, FileOutputStream}
import java.nio.file.Files

import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

trait UnitSpec extends WordSpec with Matchers {

  def writeResourceToTempDirs(dirPrefix: String, paths: String* ): File = {
    val dir = Files.createTempDirectory(dirPrefix).toFile

    paths foreach { path =>
      val prefix = FilenameUtils.getBaseName(path)
      val suffix = {
        val e = FilenameUtils.getExtension(path)
        if (e == "") ".data" else "." + e
      }

      val tf = File.createTempFile(prefix,suffix,dir)
      val os = new FileOutputStream(tf)
      val is = getClass.getResourceAsStream(path)
      IOUtils.copy(is,os)
      os.close()
      tf
    }

    dir
  }

}

trait SparkUnitSpec extends UnitSpec with BeforeAndAfterAll {

  private var _spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  lazy val spark: SparkSession = {
    if(_spark == null) {
      _spark = SparkSession.builder()
        .master("local[*]")
        .appName(getClass.getSimpleName)
          //.enableHiveSupport()
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
