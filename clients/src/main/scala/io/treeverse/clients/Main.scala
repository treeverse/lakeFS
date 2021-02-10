package io.treeverse.clients


import org.apache.spark.sql.SparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("yoni").master("local").config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
    val data = spark.range(0, 5)
    data.write.format("delta").save("/tmp/delta-table")
    print("w")
  }
}
