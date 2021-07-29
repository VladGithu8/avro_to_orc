package converter

import org.apache.spark.sql.SparkSession

object AvroToOrcSpark extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("App")
    .getOrCreate()

  val dataFrame = spark.read.format("avro").load("resources/testdata.avro")

  dataFrame.printSchema()
  dataFrame.show(10)

//  dataFrame.write.format("orc").save("resources/output/orc")
  dataFrame.write.mode("overwrite").option("compression","zlib").format("orc")
    .orc("resources/output/orc/data-zlib.orc")

//  println("--------------------------------------------------------------------------")
//  val string1 = dataFrame.collectAsList().size()
//  val string2 = spark
//    .read
//    .format("orc")
//    .orc("resources/output/orc/part-00000-86689e48-fa3c-4aa5-bb95-99d3a9e7dcd3-c000.snappy.orc")
//    .collectAsList().size()
//  println(string1 == string2)

}


