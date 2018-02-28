package demo

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: <file>")
//      System.exit(1)
//    }
//
//    val conf = new SparkConf()
//    val sc = new SparkContext("local","wordcount",conf)
//    val line = sc.textFile(args(0))
//
//    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
//
//    sc.stop()
    val conf = new SparkConf()
    val sc = new SparkContext("local","wordcount",conf)
    val rawUserArtistData = sc.textFile("hdfs://localhost:8084/Hadoop/user/Gary/work/Project1-master")
    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
//    rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
//    println(rawUserArtistData)
//    val rawUserArtistData = sc.textFile("file:///F://data//Project1-master/user_artist_data_small.txt")
//    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
//    rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
  }

//  -server
//  -Xms256m
//  -Xmx256m
}
