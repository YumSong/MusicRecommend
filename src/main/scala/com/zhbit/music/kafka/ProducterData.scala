package com.zhbit.music.kafka

import java.util.{Date, Properties}

import com.zhbit.music.core.KafkaPro
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.codehaus.jettison.json.JSONObject

import scala.util.Random

class ProducterData {

  val prop = new Properties()

  prop.put("metadata.broker.list",KafkaPro.BROKERS)

  prop.put("serializer.class",KafkaPro.ENCODER)

  val kafaConfig = new ProducerConfig(prop)

  val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"
  )

  val os_types = Array("Andorid","IOS","WINDOWS","LINUX")

  val random = new Random()

  var porint = -1

  val topics = KafkaPro.TOPIC.split("\\,").toSet

  val producter = new Producer[String,String](kafaConfig)


  def sendData(): Unit ={

    while (true){

      sendUserData()

      sendProData()

      Thread.sleep(1000)

    }

  }


  def sendUserData(): Unit ={

    val event = new JSONObject()

    event
      .put("uid",getUserID())
      .put("event_time",System.currentTimeMillis.toString)
      .put("os_type",os_types(random.nextInt(4)))
      .put("click_count",click())

    producter.send(new KeyedMessage[String,String](topics.toArray.apply(0),event.toString()))

    println("Message sendUserData: " + event)

  }


  def sendProData(): Unit ={

    val event = new JSONObject()

    event
      .put("os_type",os_types(random.nextInt(4)))
      .put("click_count",click())
      .put("event_time",new Date())

    producter.send(new KeyedMessage[String,String](topics.toArray.apply(1),event.toString()))

    println("Message sendProData: " + event)

  }


  def getUserID():String ={

    porint += 1

    if(porint >= users.length){

      porint = 0

      users(porint)

    }else{

      users(porint)

    }

  }


  def click():Double = {

    random.nextInt(10)

  }

}
