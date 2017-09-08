package com.zhbit.music.mongodb

import java.util.Date

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.codehaus.jettison.json.JSONObject

class MongodbAction {

  val mongodbClent = MongoClient("localhost",27017)

  val db = mongodbClent("datamanager")

  val userDb = db("aggregate")

  val businessDb = db("business")


  def insertUserData(str:String): Unit ={

    val json = new JSONObject(str)

    val dataUser = MongoDBObject(

      "uid"         -> json.get("uid").toString,

      "event_time"  -> json.get("event_time"),

      "os_type"     -> json.get("os_type"),

      "click_count" -> json.get("click_count")
    )

    userDb.insert(dataUser)

  }


  def insertProData(str:String): Unit ={

    val json = new JSONObject(str)

    val dataPro = MongoDBObject(

      "os_type"  -> json.get("os_type"),

      "click_count"     -> json.get("click_count"),

      "event_time" -> json.get("event_time")
    )

    businessDb.insert(dataPro)

  }

}
