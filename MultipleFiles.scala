package com.demo.spark

import org.apache.spark._
import org.apache.log4j._

object MultipleFiles {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","multiplefiles")

    val input = sc.textFile("hdfs:///user/maria_dev/spark/first.txt,hdfs:///user/maria_dev/spark/second.txt,hdfs:///user/maria_dev/spark/third.txt")

    input.collect.foreach(f=>println(f))
  }

}