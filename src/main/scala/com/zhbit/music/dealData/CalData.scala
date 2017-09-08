package com.zhbit.music.dealData

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CalData {

  val getData = new GetDataFdfs

  val allData = getData.getDataRawUserArtist()


  /**
    * 根据数据获取模型
    * ALS最小二乘法分解数据矩阵获取预测的评估函数
    * ALS 参数的解释
    * • rank = 10模型的潜在因素的个数,即“用户 - 特征”和“产品 - 特征”矩阵的列数;一般来说,它也是矩阵的阶。
    * • iterations = 5 矩阵分解迭代的次数;迭代的次数越多,花费的时间越长,但分解的结果可能会更好。
    * • lambda = 0.01 标准的过拟合参数;值越大越不容易产生过拟合,但值太大会降低分解的准确度。
    * • alpha = 1.0 控制矩阵分解时,被观察到的“用户 - 产品”交互相对没被观察到的交互的权重。
    * @param data
    * @return
    */
  def getModel(data:RDD[Rating]): MatrixFactorizationModel ={

    val model = ALS.trainImplicit(data,10,5,0.01,1.0)

    model

  }


  /**
    * 预测函数，向每个用户推荐播放最多的艺术家
    * @param sc
    * @param train
    * @param allData
    * @return
    */
  def predictMostListened(sc:SparkContext,train:RDD[Rating])(allData:RDD[(Int,Int)]) = {

    //产品的评分
    val blistenCount = sc.broadcast(train.map( r => (r.product,r.rating)).reduceByKey(_+_).collectAsMap())

    allData.map{ case (user,product) =>

      Rating(user,product,blistenCount.value.getOrElse(product,0.0))

    }

  }


  /**
    * 展现一个用户的数据
    */
  def showArtistByUser(): Unit ={

    val rawArtistsForUser = getData.rawUserArtistData.map(_.split(" ")).filter{

      case Array(user,_,_) => user.toInt == 2093760

    }

    val existingProducts = rawArtistsForUser.map{

      case Array(_,artist,_) => artist.toInt

    }.collect().toSet

    getData.getDataArtistByID().filter{

      case (id,name) => existingProducts.contains(id)

    }.values.collect().foreach(println)

  }


  /**
    * 检查模型的结果
    */
  def checkModel(): Unit ={

    val recommendations = getModel(allData).recommendProducts(2093760,5)

    val recommendedProductId = recommendations.map(_.product).toSet

    getData.getDataArtistByID().filter{

      case (id,name) => recommendedProductId.contains(id)

    }.values.collect().foreach(println)

  }


  /**
    * 根据auc评价模型的性能
    */
  def evaluateModel1(): Unit ={

    val Array(trainData,cvData) = allData.randomSplit(Array(0.9,0.1))

    trainData.cache()

    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()

    val bAllItemIDs = getData.sc.broadcast(allItemIDs)

//    val model = getModel(trainData)

//    val auc = areaUnderCurve(cvData,bAllItemIDs,model.predict)

    val auc = areaUnderCurve(cvData,bAllItemIDs,predictMostListened(getData.sc,trainData))

    println(auc)

  }

  def evaluateModel2(): Unit ={

    val Array(trainData,cvData) = allData.randomSplit(Array(0.9,0.1))

    trainData.cache()

    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()

    val bAllItemIDs = getData.sc.broadcast(allItemIDs)

    val evaluations =

      for( rank <- Array(10,50) ; lamda <- Array(1.0 , 0.0001) ; alpha <- Array(1.0 , 40.0))

      yield {

        val model = ALS.trainImplicit( trainData , rank , 5 , lamda , alpha)

        val auc = areaUnderCurve( cvData , bAllItemIDs ,model.predict)

        ((rank,lamda,alpha),auc)
      }

    evaluations.sortBy(_._2).reverse.foreach(println)

  }


  /**
    * 根据测验集检验预测的的AUC
    * @param postiveData 测试集函数
    * @param bAllItemIDs 所有的产品
    * @param predictFunction 预测函数
    * @return
    */
  def areaUnderCurve(postiveData:RDD[Rating],bAllItemIDs:Broadcast[Array[Int]],predictFunction:(RDD[(Int,Int)] => RDD[Rating])):Double ={

    //将测试集的分成用户和产品
    val postiveUserProduct = postiveData.map{r => (r.user,r.product)}

    //预测测试集中每个人所对应的产品
    val postivePredictions = predictFunction(postiveUserProduct).groupBy(_.user)

    //不属于这个用户的产品
    val negativeUserAndProduct = postiveUserProduct.groupByKey().mapPartitions{

      userIDandPostItemIDs => {

        val randow = new Random()

        val allItemIDs = bAllItemIDs.value

        userIDandPostItemIDs.map{

          case (userID,postItemIDs) =>

            val postItemIDsSet = postItemIDs.toSet

            val negative = new ArrayBuffer[Int]()

            var i = 0

            while(i < allItemIDs.size && negative.size < postItemIDsSet.size){

              val itemIDs = allItemIDs(randow.nextInt(allItemIDs.size))

              if(!postItemIDsSet.contains(itemIDs)){

                negative += itemIDs

              }

              i += 1

            }

            negative.map{ itemID => (userID,itemID)}

        }

      }.flatMap(t => t)

    }

    //对每个用户的不属于这个用户的产品做一个预测
    val negativePredictions = predictFunction(negativeUserAndProduct).groupBy(_.user)

    //预测用户用过这个产品和不用过这个产品的评分（用过这个产品的评分要比没用过的高）
    postivePredictions.join(negativePredictions).values.map{

      case (postiveRatings,negativeRatings) =>

        var correct = 0l

        var total = 0l

        for(postive <- postiveRatings; negative <- negativeRatings){

          if(postive.rating > negative.rating){

            correct +=1

          }

          total += 1

        }

        correct.toDouble/total

    }.mean()

  }


}
