import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

object AvroToOrcTest

  class AvroOrcTest extends AnyFunSuite {

    def getFilesFromDirectory(directory: String):List[File] = {
      val d = new File(directory)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("App")
      .getOrCreate()

    test("Assert first 10 records test") {
      val fileList: List[File] = getFilesFromDirectory("resources/output/orc/data-zlib.orc")
      val orcFile = fileList.filter(file => file.getName.endsWith(".orc"))

      val pathToFile = orcFile.toString()
        .replace("List", "")
        .replace("(","")
        .replace(")", "")

      val dataFrameOrc = spark.read.orc(pathToFile)
      val dataFrameAvro = spark.read.format("avro").load("resources/testdata.avro")

      val firstTenAvroList = dataFrameAvro.takeAsList(10)
      val firstTenOrcList = dataFrameOrc.takeAsList(10)

      println(firstTenAvroList.toString)
      println(firstTenOrcList.toString)

      assert(firstTenAvroList == firstTenOrcList)

    }

    test("Files count test") {
      val fileList: List[File] = getFilesFromDirectory("resources/output/orc/data-zlib.orc")
      fileList.foreach(println)
      assert(fileList.size == 4)
    }

}





