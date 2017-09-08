import java.util.Date

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

import scala.util.Random

object MongodbTest {

  val mongodbClent = MongoClient("localhost",27017)

  val db = mongodbClent("datamanager")

  val coll = db("aggregate")

  val ramdom = new Random()

  def main(args: Array[String]): Unit = {


//    (1 to 1000).foreach(insertData)

    findOrRMData(false)

  }


  def insertData(i:Int): Unit ={

    val dataTest = MongoDBObject(
      "callCount" -> ramdom.nextInt(100),
      "businessId" -> ramdom.nextInt(1000),
      "date" -> new Date()
    )

    coll.insert(dataTest)

  }


  def findOrRMData(i:Boolean): Unit ={

    i match {

      case true  => {

        val allDoc = coll.find()

        allDoc.foreach(println)

      }

      case false => {

        val allDoc = coll.find()

        allDoc.foreach(one =>coll.remove(one))

      }

    }
  }

}
