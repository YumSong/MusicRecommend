package com.zhbit.music.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Connection2Spark {

  private var SPARK_URL = "spark://datanode1:7077"

  private val EXECUTOR_MEMORY = "spark.executor.memory"

  private val DRIVER_MEMORY = "spark.driver.memory"

  private val CORE_MAX = "spark.cores.max"

  private val APP_NAME = "Test"

  private val JAR_PATH = "/home/song/IdeaProjects/MusicRecommend/out/artifacts/musicrecommend_jar/musicrecommend.jar"


  def getSc(appName:String): SparkContext ={

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(List(JAR_PATH))
      .setMaster(SPARK_URL)
      .set(EXECUTOR_MEMORY, "1g")
      .set(DRIVER_MEMORY, "1g")
      .set(CORE_MAX,"1")

    new SparkContext(conf)

  }


  def getSc(): SparkContext = {

    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .setJars(Array(JAR_PATH))
      .setMaster(SPARK_URL)
      .set(EXECUTOR_MEMORY, "1g")
      .set(DRIVER_MEMORY, "4g")
      .set(CORE_MAX, "1")

     new SparkContext(conf)

  }


  def getSsc(appName:String):StreamingContext = {

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(SPARK_URL)

    new StreamingContext(conf,Seconds(5))

  }

  def setUrl(url:String): Unit ={  this.SPARK_URL = url }

}
