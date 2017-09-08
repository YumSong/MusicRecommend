package com.zhbit.music.kafka

import com.zhbit.music.core.{Connection2Spark, KafkaPro}
import com.zhbit.music.mongodb.MongodbAction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


class Data2Streaming {

  Connection2Spark.setUrl("local[2]")

  @transient
  val ssc = Connection2Spark.getSsc("Data2Streaming")

  val mongoAction = new MongodbAction

//  val topics = KafkaPro.TOPIC.split("\\,").toSet

  val topicsUser = Set(KafkaPro.TOPIC.split("\\,").toSeq(0))

  val topicsPro = Set(KafkaPro.TOPIC.split("\\,").toSeq(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "datanode1:9092,datanode2:9092,datanode3:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "getDataFKafka",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def getData(): Unit ={

    dealUserStream()

    dealProStream()

    ssc.start()

    ssc.awaitTermination()

  }


  def dealUserStream(): Unit ={

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsUser, kafkaParams)
    )

    stream.foreachRDD { rdd =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>

        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

      }

      val newRdd = rdd.map{

        line =>

          line.value()

      }

      newRdd.collect().foreach(s => mongoAction.insertUserData(s))

    }
  }


  def dealProStream(): Unit ={

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsPro, kafkaParams)
    )

    stream.foreachRDD { rdd =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>

        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

      }

      val newRdd = rdd.map{

        line =>

          line.value()

      }

      newRdd.collect().foreach(s => mongoAction.insertProData(s))

    }
  }



}
