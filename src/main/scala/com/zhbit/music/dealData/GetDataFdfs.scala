package com.zhbit.music.dealData

import com.zhbit.music.core.Connection2Spark
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

class GetDataFdfs extends Serializable{

  @transient
  val sc = Connection2Spark.getSc("getDataFdfs")

  val rawUserArtistData = sc.textFile("hdfs://datanode1:9000/user/ds/user_artist_data.txt",12)

  //艺术家Id的rdd
  val rawArtistData = sc.textFile("hdfs://datanode1:9000/user/ds/artist_data.txt",12)

  //艺术家别名的rdd
  val rawArtistAlias = sc.textFile("hdfs://datanode1:9000/user/ds/artist_alias.txt",12)

  //艺术家别名的缓存
  val bArtistAlias = sc.broadcast(getDataArtistAlias())


  def getDataArtistByID(): RDD[(Int, String)] ={

    //艺术家Id 处理后的rdd
    val artistByID = rawArtistData.flatMap{ line =>

      val (id,name) = line.span(_!='\t')

      if( name.isEmpty ){ None }

      else {

        try{ Some((id.toInt,name.trim)) }

        catch { case e:NumberFormatException => None}

      }

    }

    artistByID

  }


  def getDataRawUserArtist(): RDD[Rating] ={

    val trainData = rawUserArtistData.map{ line =>

      val Array(userID,artistId,count) = line.split(" ").map(_.toInt)

      val finalArtistId = bArtistAlias.value.getOrElse(artistId,artistId)

      Rating(userID,finalArtistId,count)

    }.cache()

    trainData

  }


  def getDataArtistAlias(): collection.Map[Int, Int] = {

    //艺术家别名的rdd
    val artistAlias = rawArtistAlias.flatMap{ line =>

      val tokens = line.split('\t')

      if( tokens(0).isEmpty){ None }

      else { Some((tokens(0).toInt,tokens(1).toInt)) }

    }.collectAsMap()

    artistAlias

  }

}
