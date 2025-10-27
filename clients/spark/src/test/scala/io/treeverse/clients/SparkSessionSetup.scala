package io.treeverse.clients

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

trait SparkSessionSetup {
  def withSparkSession(testMethod: (SparkSession) => Any) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
      .set("spark.sql.shuffle.partitions", "17")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = new SparkSession.Builder().config(conf).getOrCreate
    testMethod(spark)
    // TODO(ariels): Can/should we "finally spark.stop()" just once, at the
    //     end of the entire suite?
  }
}
