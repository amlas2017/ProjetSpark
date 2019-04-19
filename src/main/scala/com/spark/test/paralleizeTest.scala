package com.spark.test


import org.apache.spark.sql.SparkSession

object paralleizeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext
    .parallelize(List("hello world", "bonjour le monde"))

    val words = lines.flatMap(line => line.split(" "))

    words.collect().foreach(println)
  }
}
