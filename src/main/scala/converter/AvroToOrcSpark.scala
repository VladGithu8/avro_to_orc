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
  dataFrame.show()

//  dataFrame.write.format("orc").save("resources/output/orc")
  dataFrame.write.mode("overwrite").option("compression","zlib")
    .orc("resources/output/orc/data-zlib.orc")

  spark.close()

}


