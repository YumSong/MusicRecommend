package com.zhbit.music

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SparkPi {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf().setAppName("Spark Pi Test").setMaster("spark://datanode1:7077")

      .setJars(List("/home/song/IdeaProjects/MusicRecommend/out/artifacts/musicrecommend_jar/musicrecommend.jar"))

    val spark = new SparkContext(conf)

    val slices = if (args.length > 0) args(0).toInt else 2

    val n = 100000 * slices

    val random = new Random()

    val count = spark.parallelize(1 to n, slices).map { i =>

      val x = random.nextInt() * 2 - 1

      val y = random.nextInt() * 2 - 1

      if (x * x + y * y < 1) 1 else 0

    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)

    spark.stop()

  }
}
