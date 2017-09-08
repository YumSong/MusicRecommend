package com.zhbit.music.core

object KafkaPro {

  val TOPIC = "aggregate,bussinessCallDate"

  val BROKERS = "192.168.0.225:9092,192.168.0.226:9092,192.168.0.227:9092"

  val ENCODER = "kafka.serializer.StringEncoder"

}
